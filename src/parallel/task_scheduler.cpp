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
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

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
      current_thread_count(1), requested_process_count(0), current_process_count(0), allocator(), mem_stream(allocator),
      serializer(make_uniq<BinarySerializer>(mem_stream)), deserializer(make_uniq<BinaryDeserializer>(mem_stream)) {
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

void TaskScheduler::BlockingRead(int read_fd, void *buf, size_t len) {

	// read the desired in out, until reading the desired length
	size_t bytes_read = 0;
	data_t *ptr = static_cast<data_t *>(buf);

	while (bytes_read < len) {
		ssize_t ret = read(read_fd, ptr + bytes_read, len - bytes_read);

		if (ret < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw InternalException("Error! Blocking read failed!");
		}

		if (ret == 0) {
			break;
		}

		bytes_read += ret;
	}
}

void TaskScheduler::BlockingWrite(int write_fd, const void *buf, size_t len) {

	// write the desired bytes out, until writing the desired length
	size_t bytes_written = 0;
	const data_t *ptr = static_cast<const data_t *>(buf);

	while (bytes_written < len) {
		ssize_t ret = write(write_fd, ptr + bytes_written, len - bytes_written);

		if (ret < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw InternalException("Error! Blocking read failed!");
		}

		if (ret == 0) {
			throw InternalException("Error! Didn't read the full payload!");
		}

		bytes_written += ret;
	}
}

void TaskScheduler::ExecuteUDFOnWorkers(DataChunk &chunk, idx_t function_index, Vector &result) {

	LogicalType return_type = db.func_return_types[function_index];
	for (auto &proc : processes) {
		// Send the UDF command to all processes
		BlockingWrite(proc.to_child[1], &CALL_UDF_COMMAND, sizeof(WorkerCommand));
	}

	idx_t num_procs = processes.size();

	// duplicate the chunk for each process
	vector<DataChunk> split_chunks(num_procs);
	for (idx_t i = 0; i < num_procs; ++i) {
		split_chunks[i].Initialize(allocator, chunk.GetTypes());
		split_chunks[i].Reference(chunk);
	}

	// now slice each chunk to have access to the correct range of rows
	for (idx_t i = 0; i < num_procs; ++i) {
		idx_t start_row = i * chunk.size() / num_procs;
		idx_t end_row = (i + 1) * chunk.size() / num_procs;
		idx_t num_rows = end_row - start_row;
		split_chunks[i].Slice(start_row, num_rows);
	}

	// send each process its partial chunk
	for (idx_t i = 0; i < num_procs; ++i) {

		// serialize the input data chunk
		serializer->Begin();
		split_chunks[i].Serialize(*serializer);
		serializer->End();

		// get the length and buffer
		idx_t length = mem_stream.GetPosition();
		auto *data = mem_stream.GetData();

		// reset the stream
		mem_stream.Rewind();

		// send the function index
		BlockingWrite(processes[i].to_child[1], &function_index, sizeof(idx_t));

		// send the length of the payload
		BlockingWrite(processes[i].to_child[1], &length, sizeof(idx_t));

		// then send the payload
		BlockingWrite(processes[i].to_child[1], data, length);
	}

	// receive the partial result from each process and combine them
	for (idx_t i = 0; i < num_procs; ++i) {

		// compute range for process
		idx_t start_row = i * chunk.size() / num_procs;
		idx_t end_row = (i + 1) * chunk.size() / num_procs;
		idx_t num_rows = end_row - start_row;

		// read the payload length
		idx_t payload_length = DConstants::INVALID_INDEX;
		BlockingRead(processes[i].from_child[0], &payload_length, sizeof(idx_t));

		// allocate a buffer for the payload
		vector<data_t> payload(payload_length);

		// read the result vector
		BlockingRead(processes[i].from_child[0], payload.data(), payload_length);

		// write the payload into the memory stream
		mem_stream.WriteData(payload.data(), payload_length);
		mem_stream.Rewind();

		// deserialize the partial result
		Vector partial_result(return_type);
		deserializer->Begin();
		partial_result.Deserialize(*deserializer, num_rows);
		deserializer->End();

		// reset the stream
		mem_stream.Rewind();

		// now copy the partial result from this process into the final result
		VectorOperations::Copy(partial_result, result, num_rows, 0, start_row);
	}
}

void TaskScheduler::WorkerCallUDF(int read_fd, int write_fd) {

	idx_t function_index = DConstants::INVALID_INDEX;
	idx_t payload_length = DConstants::INVALID_INDEX;

	// read the function index
	BlockingRead(read_fd, &function_index, sizeof(idx_t));

	// read the payload length
	BlockingRead(read_fd, &payload_length, sizeof(idx_t));

	// allocate a buffer for the payload
	vector<data_t> payload(payload_length);

	// read in the payload
	BlockingRead(read_fd, payload.data(), payload_length);

	// write the payload into the memory stream
	mem_stream.WriteData(payload.data(), payload_length);
	mem_stream.Rewind();

	// deserialize it into the input chunk
	DataChunk input;
	deserializer->Begin();
	input.Deserialize(*deserializer);
	deserializer->End();
	// reset the stream
	mem_stream.Rewind();

	// call the UDF
	inner_scalar_function_t &func = db.funcs[function_index];
	LogicalType func_return_type = db.func_return_types[function_index];
	Vector result(func_return_type);
	func(input, result);

	// serialize the resulting output vector
	serializer->Begin();
	result.Serialize(*serializer, input.size());
	serializer->End();
	idx_t output_length = mem_stream.GetPosition();
	mem_stream.Rewind();
	auto *output_data = mem_stream.GetData();

	// send the result vector back
	// first write out the payload length
	BlockingWrite(write_fd, &output_length, sizeof(idx_t));

	// then write out the payload
	BlockingWrite(write_fd, output_data, output_length);
}

void TaskScheduler::WorkerExit(int read_fd, int write_fd) {
	close(read_fd);
	close(write_fd);
	_exit(0);
}

void TaskScheduler::RunWorkerProcess(int read_fd, int write_fd) {
	// wait for work
	WorkerCommand command;
	while (true) {
		// safe read
		ssize_t n = read(read_fd, &command, sizeof(WorkerCommand));
		if (n <= 0) {
			break;
		}
		switch (command) {
		case WorkerCommand::CALL_UDF:
			WorkerCallUDF(read_fd, write_fd);
			break;
		case WorkerCommand::EXIT:
			WorkerExit(read_fd, write_fd);
			break;
		}
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
		idx_t process_count = processes.size();
		for (idx_t i = 0; i < process_count; ++i) {
			// kill the child process by writing an exit message
			BlockingWrite(processes[i].to_child[1], &EXIT_COMMAND, sizeof(WorkerCommand));
			close(processes[i].to_child[1]);   // close parent -> child write pipe
			close(processes[i].from_child[0]); // close child -> parent read pipe
			// wait for it to terminate
			int status;
			waitpid(processes[i].pid, &status, 0);
		}
		processes.clear();
	}
	if (processes.size() < new_process_count) {
		// we are increasing the number of processes: launch them and run tasks on them
		idx_t create_new_processes = new_process_count;
		// allocate space for each new process state
		processes.resize(create_new_processes);
		for (idx_t i = 0; i < create_new_processes; i++) {
			// create parent to child and child to parent pipes
			if (pipe(processes[i].to_child) == -1 || pipe(processes[i].from_child) == -1) {
				// can't create more processes
				break;
			}
		}

		for (idx_t i = 0; i < create_new_processes; i++) {
			// now fork a new processes
			pid_t pid = fork();
			if (pid < 0) {
				// error creating a new process
				break;
			}

			// child process
			if (pid == 0) {
				close(processes[i].to_child[1]);   // close parent -> child write pipe
				close(processes[i].from_child[0]); // close child -> parent read pipe

				// close other unrelated pipes
				for (idx_t j = 0; j < create_new_processes; j++) {
					if (i == j) {
						continue;
					}
					close(processes[j].to_child[0]);
					close(processes[j].to_child[1]);
					close(processes[j].from_child[0]);
					close(processes[j].from_child[1]);
				}

				// now go and wait for work
				RunWorkerProcess(processes[i].to_child[0], processes[i].from_child[1]);
			}
			// parent process
			else {
				processes[i].pid = pid;
				close(processes[i].to_child[0]);   // close parent -> child read pipe
				close(processes[i].from_child[1]); // close child -> parent write pipe
			}
		}
	}
	current_process_count = processes.size();
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
}

} // namespace duckdb
