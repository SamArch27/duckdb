//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <linux/futex.h>
#include <sys/syscall.h>

namespace duckdb {

struct ConcurrentQueue;
struct QueueProducerToken;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;
class MemoryStream;

struct SchedulerThread;

struct ProducerToken {
	ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token);
	~ProducerToken();

	TaskScheduler &scheduler;
	unique_ptr<QueueProducerToken> token;
	mutex producer_lock;
};

static inline void futex_wait(atomic<int> *f, int expected) {
	syscall(SYS_futex, (int *)f, FUTEX_WAIT, expected, NULL, NULL, 0);
}

static inline void futex_wake(atomic<int> *f) {
	syscall(SYS_futex, (int *)f, FUTEX_WAKE, 1, NULL, NULL, 0);
}

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {

	// timeout for semaphore wait, default 5ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 5000;
	constexpr static idx_t SHM_BUFFER_SIZE = 8 * 1024 * 1024;
	constexpr static idx_t EXIT_FUNCTION_INDEX = DConstants::INVALID_INDEX;

	struct SharedWorkerBlock {
		alignas(64) atomic<int> futex_cmd;  // parent -> worker (command signal)
		alignas(64) atomic<int> futex_done; // worker -> parent (completion signal)
		alignas(64) idx_t function_index;

		alignas(64) data_ptr_t input_buffer;               // parent writes, worker reads
		alignas(64) data_t output_buffer[SHM_BUFFER_SIZE]; // worker writes, parent reads
	};

	struct ProcessState {
		pid_t pid;
		int shm_fd;
		SharedWorkerBlock *shared_block;
	};

public:
	explicit TaskScheduler(DatabaseInstance &db);
	~TaskScheduler();

	DUCKDB_API static TaskScheduler &GetScheduler(ClientContext &context);
	DUCKDB_API static TaskScheduler &GetScheduler(DatabaseInstance &db);

	unique_ptr<ProducerToken> CreateProducer();
	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(ProducerToken &producer, shared_ptr<Task> task);
	//! Fetches a task from a specific producer, returns true if successful or false if no tasks were available
	bool GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	//! Run tasks forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	void ExecuteForever(atomic<bool> *marker);
	//! Run tasks until `marker` is set to false, `max_tasks` have been completed, or until there are no more tasks
	//! available. Returns the number of tasks that were completed.
	idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks);
	//! Run tasks until `max_tasks` have been completed, or until there are no more tasks available
	void ExecuteTasks(idx_t max_tasks);

	//! Sets the amount of background threads to be used for execution, based on the number of total threads
	//! and the number of external threads. External threads, e.g. the main thread, will also be used for execution.
	//! Launches `total_threads - external_threads` background worker threads.
	void SetThreads(idx_t total_threads, idx_t external_threads);
	void RelaunchThreads();

	//! Sets the amount of background processes for UDF execution, based on the total requested
	void SetProcesses(idx_t total_processes);
	void RelaunchProcesses();

	//! Returns the number of threads
	DUCKDB_API int32_t NumberOfThreads();

	idx_t GetNumberOfTasks() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;

	//! Send signals to n threads, signalling for them to wake up and attempt to execute a task
	void Signal(idx_t n);

	//! Yield to other threads
	static void YieldThread();

	//! Set the allocator flush threshold
	void SetAllocatorFlushTreshold(idx_t threshold);
	//! Sets the allocator background thread
	void SetAllocatorBackgroundThreads(bool enable);

	//! Get the number of the CPU on which the calling thread is currently executing.
	//! Fallback to calling thread id if CPU number is not available.
	//! Result do not need to be exact 'return 0' is a valid fallback strategy
	static idx_t GetEstimatedCPUId();

	void ExecuteUDFOnParallelWorkers(DataChunk &chunk, idx_t function_index, Vector &result);
	void RunWorkerProcess(SharedWorkerBlock *block, int shm_fd, idx_t worker_index);

private:
	void SerializeVector(MemoryStream &stream, Vector &vec, idx_t count, LogicalTypeId type);
	void SerializeDataChunk(MemoryStream &stream, DataChunk &chunk);
	void DeserializeDataChunk(MemoryStream &stream, DataChunk &chunk);

	void RelaunchThreadsInternal(int32_t n);
	void RelaunchProcessesInternal(int32_t n);

private:
	DatabaseInstance &db;
	//! The task queue
	unique_ptr<ConcurrentQueue> queue;
	//! Lock for modifying the thread count
	mutex thread_lock;
	//! Lock for modifying the process count
	mutex process_lock;
	//! The active background threads of the task scheduler
	vector<unique_ptr<SchedulerThread>> threads;
	//! The active python process state for each process of the task scheduler
	vector<ProcessState> processes;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<atomic<bool>>> markers;
	//! The threshold after which to flush the allocator after completing a task
	atomic<idx_t> allocator_flush_threshold;
	//! Whether allocator background threads are enabled
	atomic<bool> allocator_background_threads;
	//! Requested thread count (set by the 'threads' setting)
	atomic<int32_t> requested_thread_count;
	//! The amount of threads currently running
	atomic<int32_t> current_thread_count;
	//! Requested processes count (set by the 'python_processes' setting)
	atomic<int32_t> requested_process_count;
	//! The amount of processes currently running
	atomic<int32_t> current_process_count;
	//! Allocator
	Allocator allocator;
};

} // namespace duckdb
