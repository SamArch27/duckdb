#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

#include <thread>
#else
#include <queue>
#endif

#if defined(_WIN32)
#include <windows.h>
#elif defined(__GNUC__)
#include <sched.h>
#include <unistd.h>
#endif
#include <algorithm>
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include <sys/mman.h>

namespace duckdb {

struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
#endif
};

#ifndef DUCKDB_NO_THREADS
typedef duckdb_moodycamel::ConcurrentQueue<shared_ptr<Task>> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	concurrent_queue_t q;
	lightweight_semaphore_t semaphore;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue_token(queue.q) {
	}

	duckdb_moodycamel::ProducerToken queue_token;
};

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	if (q.enqueue(token.token->queue_token, std::move(task))) {
		semaphore.signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	return q.try_dequeue_from_producer(token.token->queue_token, task);
}

#else
struct ConcurrentQueue {
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q;
	mutex qlock;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
};

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	q[std::ref(*token.token)].push(std::move(task));
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	D_ASSERT(!q.empty());

	const auto it = q.find(std::ref(*token.token));
	if (it == q.end() || it->second.empty()) {
		return false;
	}

	task = std::move(it->second.front());
	it->second.pop();

	return true;
}

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue(&queue) {
	}

	~QueueProducerToken() {
		lock_guard<mutex> lock(queue->qlock);
		queue->q.erase(*this);
	}

private:
	ConcurrentQueue *queue;
};
#endif

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {
}

ProducerToken::~ProducerToken() {
}

TaskScheduler::TaskScheduler(DatabaseInstance &db)
    : db(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(db.config.options.allocator_background_threads), requested_thread_count(0),
      current_thread_count(1), requested_process_count(0), current_process_count(0), allocator() {
	SetAllocatorBackgroundThreads(db.config.options.allocator_background_threads);
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		RelaunchThreadsInternal(0);
		RelaunchProcessesInternal(0);
	} catch (...) {
		// nothing we can do in the destructor if this fails
	}
#endif
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

unique_ptr<ProducerToken> TaskScheduler::CreateProducer() {
	auto token = make_uniq<QueueProducerToken>(*queue);
	return make_uniq<ProducerToken>(*this, std::move(token));
}

void TaskScheduler::ScheduleTask(ProducerToken &token, shared_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(token, std::move(task));
}

bool TaskScheduler::GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(token, task);
}

void TaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	static constexpr const int64_t INITIAL_FLUSH_WAIT = 500000; // initial wait time of 0.5s (in mus) before flushing

	shared_ptr<Task> task;
	// loop until the marker is set to false
	while (*marker) {
		if (!Allocator::SupportsFlush()) {
			// allocator can't flush, just start an untimed wait
			queue->semaphore.wait();
		} else if (!queue->semaphore.wait(INITIAL_FLUSH_WAIT)) {
			// allocator can flush, we flush this threads outstanding allocations after it was idle for 0.5s
			Allocator::ThreadFlush(allocator_background_threads, allocator_flush_threshold,
			                       NumericCast<idx_t>(requested_thread_count.load()));
			auto decay_delay = Allocator::DecayDelay();
			if (!decay_delay.IsValid()) {
				// no decay delay specified - just wait
				queue->semaphore.wait();
			} else {
				if (!queue->semaphore.wait(UnsafeNumericCast<int64_t>(decay_delay.GetIndex()) * 1000000 -
				                           INITIAL_FLUSH_WAIT)) {
					// in total, the thread was idle for the entire decay delay (note: seconds converted to mus)
					// mark it as idle and start an untimed wait
					Allocator::ThreadIdle();
					queue->semaphore.wait();
				}
			}
		}
		if (queue->q.try_dequeue(task)) {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		}
	}
	// this thread will exit, flush all of its outstanding allocations
	if (Allocator::SupportsFlush()) {
		Allocator::ThreadFlush(allocator_background_threads, 0, NumericCast<idx_t>(requested_thread_count.load()));
		Allocator::ThreadIdle();
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

idx_t TaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	idx_t completed_tasks = 0;
	// loop until the marker is set to false
	while (*marker && completed_tasks < max_tasks) {
		shared_ptr<Task> task;
		if (!queue->q.try_dequeue(task)) {
			return completed_tasks;
		}
		auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

		switch (execute_result) {
		case TaskExecutionResult::TASK_FINISHED:
		case TaskExecutionResult::TASK_ERROR:
			task.reset();
			completed_tasks++;
			break;
		case TaskExecutionResult::TASK_NOT_FINISHED:
			throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
		case TaskExecutionResult::TASK_BLOCKED:
			task->Deschedule();
			task.reset();
			break;
		}
	}
	return completed_tasks;
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

void TaskScheduler::ExecuteTasks(idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	shared_ptr<Task> task;
	for (idx_t i = 0; i < max_tasks; i++) {
		queue->semaphore.wait(TASK_TIMEOUT_USECS);
		if (!queue->q.try_dequeue(task)) {
			return;
		}
		try {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} catch (...) {
			return;
		}
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

#ifndef DUCKDB_NO_THREADS
static void ThreadExecuteTasks(TaskScheduler *scheduler, atomic<bool> *marker) {
	scheduler->ExecuteForever(marker);
}
#endif

int32_t TaskScheduler::NumberOfThreads() {
	return current_thread_count.load();
}

idx_t TaskScheduler::GetNumberOfTasks() const {
#ifndef DUCKDB_NO_THREADS
	return queue->q.size_approx();
#else
	idx_t task_count = 0;
	for (auto &producer : queue->q) {
		task_count += producer.second.size();
	}
	return task_count;
#endif
}

idx_t TaskScheduler::GetProducerCount() const {
#ifndef DUCKDB_NO_THREADS
	return queue->q.size_producers_approx();
#else
	return queue->q.size();
#endif
}

idx_t TaskScheduler::GetTaskCountForProducer(ProducerToken &token) const {
#ifndef DUCKDB_NO_THREADS
	lock_guard<mutex> producer_lock(token.producer_lock);
	return queue->q.size_producer_approx(token.token->queue_token);
#else
	const auto it = queue->q.find(std::ref(*token.token));
	if (it == queue->q.end()) {
		return 0;
	}
	return it->second.size();
#endif
}

void TaskScheduler::SetThreads(idx_t total_threads, idx_t external_threads) {
	if (total_threads == 0) {
		throw SyntaxException("Number of threads must be positive!");
	}
#ifndef DUCKDB_NO_THREADS
	if (total_threads < external_threads) {
		throw SyntaxException("Number of threads can't be smaller than number of external threads!");
	}
#else
	if (total_threads != external_threads) {
		throw NotImplementedException(
		    "DuckDB was compiled without threads! Setting total_threads != external_threads is not allowed.");
	}
#endif
	requested_thread_count = NumericCast<int32_t>(total_threads - external_threads);
}

void TaskScheduler::SetProcesses(idx_t total_processes) {
	requested_process_count = NumericCast<int32_t>(total_processes);
}

void TaskScheduler::SetAllocatorFlushTreshold(idx_t threshold) {
	allocator_flush_threshold = threshold;
}

void TaskScheduler::SetAllocatorBackgroundThreads(bool enable) {
	allocator_background_threads = enable;
	Allocator::SetBackgroundThreads(enable);
}

void TaskScheduler::Signal(idx_t n) {
#ifndef DUCKDB_NO_THREADS
	typedef std::make_signed<std::size_t>::type ssize_t;
	queue->semaphore.signal(NumericCast<ssize_t>(n));
#endif
}

void TaskScheduler::YieldThread() {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::yield();
#endif
}

idx_t TaskScheduler::GetEstimatedCPUId() {
#if defined(EMSCRIPTEN)
	// FIXME: Wasm + multithreads can likely be implemented as
	//   return return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
	return 0;
#else
	// this code comes from jemalloc
#if defined(_WIN32)
	return (idx_t)GetCurrentProcessorNumber();
#elif defined(_GNU_SOURCE)
	auto cpu = sched_getcpu();
	if (cpu < 0) {
#ifndef DUCKDB_NO_THREADS
		// fallback to thread id
		return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else

		return 0;
#endif
	}
	return (idx_t)cpu;
#elif defined(__aarch64__) && defined(__APPLE__)
	/* Other oses most likely use tpidr_el0 instead */
	uintptr_t c;
	asm volatile("mrs %x0, tpidrro_el0" : "=r"(c)::"memory");
	return (idx_t)(c & (1 << 3) - 1);
#else
#ifndef DUCKDB_NO_THREADS
	// fallback to thread id
	return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else
	return 0;
#endif
#endif
#endif
}

void TaskScheduler::RelaunchThreads() {
	lock_guard<mutex> t(thread_lock);
	auto n = requested_thread_count.load();
	RelaunchThreadsInternal(n);
}

void TaskScheduler::RelaunchProcesses() {
	lock_guard<mutex> t(process_lock);
	auto n = requested_process_count.load();
	RelaunchProcessesInternal(n);
}

void TaskScheduler::RelaunchThreadsInternal(int32_t n) {
#ifndef DUCKDB_NO_THREADS
	auto &config = DBConfig::GetConfig(db);
	auto new_thread_count = NumericCast<idx_t>(n);
	if (threads.size() == new_thread_count) {
		current_thread_count = NumericCast<int32_t>(threads.size() + config.options.external_threads);
		return;
	}
	if (threads.size() > new_thread_count) {
		// we are reducing the number of threads: clear all threads first
		for (idx_t i = 0; i < threads.size(); i++) {
			*markers[i] = false;
		}
		Signal(threads.size());
		// now join the threads to ensure they are fully stopped before erasing them
		for (idx_t i = 0; i < threads.size(); i++) {
			threads[i]->internal_thread->join();
		}
		// erase the threads/markers
		threads.clear();
		markers.clear();
	}
	if (threads.size() < new_thread_count) {
		// we are increasing the number of threads: launch them and run tasks on them
		idx_t create_new_threads = new_thread_count - threads.size();
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get());
			} catch (std::exception &ex) {
				// thread constructor failed - this can happen when the system has too many threads allocated
				// in this case we cannot allocate more threads - stop launching them
				break;
			}
			auto thread_wrapper = make_uniq<SchedulerThread>(std::move(worker_thread));

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
	current_thread_count = NumericCast<int32_t>(threads.size() + config.options.external_threads);
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
#endif
}

void TaskScheduler::SerializeVector(MemoryStream &stream, Vector &vec, idx_t count, LogicalTypeId type) {
	// save position
	auto original_pos = stream.GetPosition();
	// now write out the vector
	switch (vec.GetType().id()) {
	case LogicalTypeId::VARCHAR: {
		// cast to array of strings and write it out
		std::cout << "Writing out vector of VARCHAR" << std::endl;
		auto *data = FlatVector::GetData<string_t>(vec);
		stream.WriteData(reinterpret_cast<const_data_ptr_t>(data), sizeof(string_t) * count);
	} break;
	default:
		throw InternalException("Trying to serialize unsupported type!");
	}

	// now clean up string data if it's not inlined
	switch (vec.GetType().id()) {
	case LogicalTypeId::VARCHAR: {
		std::cout << "Going back to update non-inlined strings!" << std::endl;
		auto *data = FlatVector::GetData<string_t>(vec);
		for (idx_t row = 0; row < count; ++row) {
			// if the string is not inlined, write it out to shared memory and fix up the original pointer
			if (!data[row].IsInlined()) {
				std::cout << "Non-inlined string found!" << std::endl;
				// store a pointer to where the string will be stored in shared memory
				const char *string_shm = reinterpret_cast<const char *>(stream.GetData() + stream.GetPosition());
				std::cout << "String is going to be written to: " << stream.GetPosition() << std::endl;
				// get the length of the string
				idx_t len = data[row].GetSize();
				std::cout << "String length is: " << len << std::endl;
				// write out the string to shared memory
				stream.WriteData(reinterpret_cast<const_data_ptr_t>(data[row].GetData()), len);
				std::cout << "Wrote string out to shared memory!" << std::endl;
				// save the stream position
				auto old_offset = stream.GetPosition();
				std::cout << "Saving old_offset as: " << old_offset << std::endl;
				// jump to the correct data element
				stream.SetPosition(original_pos + sizeof(string_t) * row);
				std::cout << "Jumping to the original string to update pointer at offset: "
				          << original_pos + sizeof(string_t) * row;
				// create a new string on the stack point to the one in shared memory
				string_t updated_string(string_shm, len);
				// write out the new updated string
				stream.WriteData(reinterpret_cast<const_data_ptr_t>(&updated_string), sizeof(string_t));
				std::cout << "Wrote out pointer to new string!" << std::endl;
				// reset the stream position
				stream.SetPosition(old_offset);
				std::cout << "Resetting cursor back to: " << old_offset << std::endl;
			}
		}
	} break;
	default:
		throw InternalException("Trying to serialize unsupported type!");
	}
}

void TaskScheduler::SerializeDataChunk(MemoryStream &stream, DataChunk &chunk) {
	// ensure that its flattened before serialization
	chunk.Flatten();

	// write the count
	idx_t count = chunk.size();
	stream.Write(count);
	std::cout << "Writing chunk size: " << count << std::endl;

	// write the number of columns
	idx_t columns = chunk.ColumnCount();
	stream.Write(columns);

	std::cout << "Writing columns: " << columns << std::endl;

	// write the types
	auto types = chunk.GetTypes();
	for (idx_t i = 0; i < columns; ++i) {
		stream.Write(static_cast<uint8_t>(types[i].id()));
		std::cout << "Writing type: " << types[i].ToString() << std::endl;
	}

	// now for each column we want to save the bytes written
	auto offset_pos = stream.GetPosition();
	std::cout << "Saving current offset of: " << offset_pos << std::endl;
	stream.SetPosition(offset_pos + columns * sizeof(idx_t));
	// save the offset into this vector
	size_t byte_offset = stream.GetPosition();
	// now for each vector, write out the vector then go back and write out its offset
	for (idx_t i = 0; i < columns; ++i) {
		std::cout << "Writing out vector at index: " << i << std::endl;
		// write out the vector
		SerializeVector(stream, chunk.data[i], count, types[i].id());
		// save the cursor
		auto new_pos = stream.GetPosition();
		std::cout << "Saving current offset of: " << new_pos << std::endl;
		// go back to the position where the bytes written should be stored
		stream.SetPosition(offset_pos + i * sizeof(idx_t));
		std::cout << "Updating offset to: " << offset_pos + i * sizeof(idx_t) << std::endl;
		// write the byte offset
		stream.Write(byte_offset);
		std::cout << "Writing byte_offset of: " << byte_offset << std::endl;
		// update the byte offset for the next vector
		byte_offset = new_pos;
		std::cout << "Updating byte_offset to: " << new_pos << std::endl;
		// reset the cursor
		stream.SetPosition(new_pos);
		std::cout << "Resetting cursor to: " << new_pos << std::endl;
	}
}

void TaskScheduler::DeserializeDataChunk(MemoryStream &stream, DataChunk &chunk) {
	// read the count
	idx_t count = stream.Read<idx_t>();
	std::cout << "Read count value of: " << count << std::endl;
	// number of columns
	idx_t columns = stream.Read<idx_t>();
	std::cout << "Read columns value of: " << columns << std::endl;

	// now read out the types
	vector<LogicalTypeId> type_ids(columns);
	stream.ReadData(reinterpret_cast<data_ptr_t>(type_ids.data()), sizeof(uint8_t) * columns);
	// now init the data chunk
	vector<LogicalType> types(columns);
	for (idx_t i = 0; i < columns; ++i) {
		types[i] = LogicalType(type_ids[i]);
		std::cout << "Read type: " << types[i].ToString() << std::endl;
	}
	chunk.InitializeEmpty(types);
	// now read out the offsets
	vector<idx_t> offsets(columns);
	stream.ReadData(reinterpret_cast<data_ptr_t>(offsets.data()), sizeof(uint8_t) * columns);
	for (auto offset : offsets) {
		std::cout << "Read offset of: " << offset << std::endl;
	}
	std::cout << "Assigning zero copy to each vec" << std::endl;
	// now "zero copy" by assigning each of the chunk's vectors to appropriate pointer + offset into shared memory
	for (idx_t i = 0; i < columns; ++i) {
		chunk.data[i].data = stream.GetData() + offsets[i];
	}
	chunk.count = count;
}

void TaskScheduler::ExecuteUDFOnParallelWorkers(DataChunk &chunk, idx_t function_index, Vector &result) {
	LogicalType return_type = db.func_return_types[function_index];
	idx_t num_procs = processes.size();

	for (idx_t i = 0; i < num_procs; ++i) {
		auto &proc = processes[i];
		auto *block = proc.shared_block;

		// serialize it into shared memory
		std::cout << "Printing input chunk!" << std::endl;
		std::cout << chunk.ToString() << std::endl;
		auto input_stream = MemoryStream(static_cast<data_ptr_t>(block->input_buffer), SHM_BUFFER_SIZE);
		SerializeDataChunk(input_stream, chunk);
		std::cout << "Serialized!" << std::endl;
		// Now try converting it BACK to a regular datachunk and see if it's correct
		input_stream.Rewind();
		DataChunk deserialized;
		DeserializeDataChunk(input_stream, deserialized);
		std::cout << "Deserialized!" << std::endl;
		std::cout << "Printing deserialized input chunk!" << std::endl;
		std::cout << deserialized.ToString() << std::endl;
		_exit(0);

		// signal the worker to process it
		block->function_index = function_index;
		block->futex_cmd.store(1, std::memory_order_release);
		futex_wake(&block->futex_cmd);
	}

	// receive the partial result from each process and combine them
	for (idx_t i = 0; i < num_procs; ++i) {
		auto &proc = processes[i];
		auto *block = proc.shared_block;

		// block until the process is done
		while (block->futex_done.load(std::memory_order_acquire) != 1) {
			futex_wait(&block->futex_done, 0);
		}

		// compute range for process
		idx_t start_row = i * chunk.size() / num_procs;
		idx_t end_row = (i == num_procs - 1) ? chunk.size() : (i + 1) * chunk.size() / num_procs;
		idx_t num_rows = end_row - start_row;

		// read back the partial result
		auto output_stream = make_uniq<MemoryStream>(static_cast<data_ptr_t>(block->output_buffer), SHM_BUFFER_SIZE);
		auto deserializer = make_uniq<BinaryDeserializer>(*output_stream);
		Vector partial_result(return_type);
		deserializer->Begin();
		partial_result.Deserialize(*deserializer, num_rows);
		deserializer->End();

		// now copy the partial result from this process into the final result
		VectorOperations::Copy(partial_result, result, num_rows, 0, start_row);

		// reset the futex
		block->futex_done.store(0, std::memory_order_release);
	}
}

void TaskScheduler::RunWorkerProcess(SharedWorkerBlock *block, int shm_fd) {
	while (true) {

		// block on futex
		while (block->futex_cmd.load(std::memory_order_acquire) != 1) {
			futex_wait(&block->futex_cmd, 0);
		}

		// check for exit signal
		if (block->function_index == EXIT_FUNCTION_INDEX) {
			// unmap shared memory region and exit
			munmap(block, sizeof(SharedWorkerBlock));
			close(shm_fd);
			_exit(0);
		}

		// read the partial input chunk
		auto input_stream = make_uniq<MemoryStream>(static_cast<data_ptr_t>(block->input_buffer), SHM_BUFFER_SIZE);
		auto deserializer = make_uniq<BinaryDeserializer>(*input_stream);
		DataChunk input;
		deserializer->Begin();
		input.Deserialize(*deserializer);
		deserializer->End();

		// call the UDF
		inner_scalar_function_t &func = db.funcs[block->function_index];
		LogicalType func_return_type = db.func_return_types[block->function_index];
		Vector output(func_return_type);
		func(input, output);

		// serialize the output to shared memory
		auto output_stream = make_uniq<MemoryStream>(static_cast<data_ptr_t>(block->output_buffer), SHM_BUFFER_SIZE);
		auto serializer = make_uniq<BinarySerializer>(*output_stream);
		serializer->Begin();
		output.Serialize(*serializer, input.size());
		serializer->End();

		// reset futex and wake parent
		block->futex_cmd.store(0, std::memory_order_release);
		block->futex_done.store(1, std::memory_order_release);
		futex_wake(&block->futex_done);
	}
}

void TaskScheduler::RelaunchProcessesInternal(int32_t n) {
	auto new_process_count = NumericCast<idx_t>(n);
	if (processes.size() == new_process_count) {
		current_process_count = new_process_count;
		return;
	}
	if (processes.size() > new_process_count) {
		// we are reducing the number of processes: kill all processes
		for (auto &proc : processes) {
			// send exit command via shared memory
			proc.shared_block->function_index = EXIT_FUNCTION_INDEX;
			proc.shared_block->futex_cmd.store(1, std::memory_order_release);
			futex_wake(&proc.shared_block->futex_cmd);

			// wait for the worker process to exit
			int status;
			waitpid(proc.pid, &status, 0);

			// unmap the shared memory region and close the fd
			munmap(proc.shared_block, sizeof(SharedWorkerBlock));
			close(proc.shm_fd);
		}

		processes.clear();
	}
	if (processes.size() < new_process_count) {
		// we are increasing the number of processes: launch them and run tasks on them
		idx_t to_create = new_process_count;
		// allocate space for each new process state
		processes.resize(to_create);
		for (idx_t i = 0; i < to_create; i++) {
			// allocate descriptors for shared memory for this worker
			ProcessState &proc = processes[i];
			proc.shm_fd = memfd_create("duckdb_worker", MFD_CLOEXEC);
			if (proc.shm_fd < 0) {
				throw InternalException("Error: mem_fd(...) failed!");
			}
			// map the actual shared memory region
			if (ftruncate(proc.shm_fd, sizeof(SharedWorkerBlock)) == -1) {
				throw InternalException("Error: ftruncate(...) failed!");
			}
			proc.shared_block = static_cast<SharedWorkerBlock *>(
			    mmap(nullptr, sizeof(SharedWorkerBlock), PROT_READ | PROT_WRITE, MAP_SHARED, proc.shm_fd, 0));

			if (proc.shared_block == MAP_FAILED) {
				throw InternalException("Error: mmap(...) failed!");
			}
			// zero out the memory
			memset(static_cast<void *>(proc.shared_block), 0, sizeof(SharedWorkerBlock));
			// zero out all of the atomics
			proc.shared_block->futex_cmd.store(0, std::memory_order_relaxed);
			proc.shared_block->futex_done.store(0, std::memory_order_relaxed);
		}

		for (idx_t i = 0; i < to_create; i++) {
			// now fork a new processes
			pid_t pid = fork();
			if (pid < 0) {
				throw InternalException("Error: fork(...) failed!");
			}

			// child process
			if (pid == 0) {
				// unmap unrelated shared memory regions for this process
				for (idx_t j = 0; j < to_create; j++) {
					if (i == j) {
						continue;
					}
					munmap(processes[j].shared_block, sizeof(SharedWorkerBlock));
					close(processes[j].shm_fd);
				}

				// now go and wait for work
				RunWorkerProcess(processes[i].shared_block, processes[i].shm_fd);
			}
			// parent process
			else {
				processes[i].pid = pid;
			}
		}
	}
	current_process_count = processes.size();
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
}

} // namespace duckdb
