#include <pybind11/embed.h>
#include <iostream>
#include <coroutine>
#include <atomic>
#include <array>
#include <utility>
#include <string>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace py = pybind11;
namespace fs = std::filesystem;

// there are two ring buffers, MPSC and SPMC
// data structures ------------------------
struct WaiterNode {
	coroutine_handle<> h;
	WaiterNode* next = nullptr;
};

struct DataBatch {
	int batch_id;
	py::object arrow_data;
};

struct ResultBatch {
	int batch_id;
	py::object processed_data;
};

template <typename T>
struct Task {
	struct promise_type {
		WaiterNode node;
		Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
		std::suspend_always initial_suspend() noexcept { return {}; } // create suspended
		std::suspend_always final_suspend() noexcept { return {}; } // keep frame alive
		void return_void() {}
		void unhandled_exception() { std::terminate(); }
	};

	std::coroutine_handle<promise_type> h;
	explicit Task(std::coroutine_handle<promise_type> h) : h(h) {}
	~Task() { if (h) h.destroy(); }
	void start() { if (h && !h.done()) h.resume(); }
};

template <typename T, size_t capacity = 1024>
class MPSC {
	// fixed size array for lock-free storage
	std::array<T, capacity> buffer;
	std::atomic<size_t> head{0}, tail{0};

	// tracks consumer coroutine waiting for data
	std::atomic<bool> data_ready{false};
	std::atomic<std::coroutine_handle<>> waiter{nullptr};
	
	public:
		static constexpr size_t mask = capacity - 1; // capacity is a power of 2 and mask is 2047 (0x7FF)
		void push(T item) {
			size_t t = tail.load(std::memory_order_relaxed);
			buffer[t & mask] = std::move(item);
			tail.store(t+1, std::memory_order_release);
			data_ready.store(true, std::memory_order_release);

			// wake up consumer if waiting
			auto h = waiter.exchange(nullptr, std::memory_order_release);
			if (h) h.resume();
		}

		struct Awaiter {
			MPSC& q;
			bool await_ready() const { return q.data_ready.load(std::memory_order_acquire); }
			void await_suspend(std::coroutine_handle<> h) { q.waiter.store(h, std::memory_order_release); } 
			T await_resume() const { 
				size_t h = q.head.load(std::memory_order_relaxed);
				T val = std::move(q.buffer[h & mask]);
				q.head.store(h+1, std::memory_order_release);
				if (q.head.load() == q.tail.load()) q.data_ready.store(false);
				return val;
			} // pop from storage and return
		};
		
		Awaiter pop() { return Awaiter{*this}; }
};

template <typename T, size_t capacity = 1024>
class SPMC {
	std::array<T, capacity> buffer;
	std::atomic<size_t> head{0}, tail{0};
	std::atomic<WaiterNode*> waiter_head{nullptr};

	public:
		static constexpr size_t mask = capacity - 1;
		void push(T item) {
			buffer[tail.load(std::memory_order_relaxed) & mask] = std::move(item);
			tail.fetch_add(1, std::memory_order_release);

			WaiterNode* node = waiter_head.exchange(nullptr, std::memory_order_acquire);
			while (node) {
				WaiterNode* next = node->next;
				node->h.resume();
				node = next;
			}
		}

		struct Awaiter {
			SPMC& q;
			bool await_ready() const { return q.head.load(std::memory_order_acquire) != q.tail.load(std::memory_order_acquire); }
			void await_suspend(std::coroutine_handle<> h) const { 
				auto& promise = std::coroutine_handle<typename Task<T>::promise_type>::from_address(h.address()).promise();
				auto& node = promise.node;
				node.h = h;
				node.next = q.waiter_head.load(std::memory_order_relaxed);
				while (!q.waiter_head.compare_exchange_weak(node.next, &node, std::memory_order_release, std::memory_order_relaxed));
			} 
			T await_resume() {
				size_t h = q.head.fetch_add(1, memory_order_acquire);
				return std::move(q.buffer[h & mask]);
			}
		};
		
		Awaiter pop() { return Awaiter{*this}; }
};

// helper functions -----------------------
std::string read_file(const std::string& file_path) {
	if (file_path.empty()) return "";
	std::ifstream file(file_path);
	if (!file.is_open()) throw std::runtime_error("Could not open file" + file_path);
	std::stringstream buffer;
	buffer << file.rdbuf();
	return buffer.str();
}

void load_udfs(const string& directory_path) {
	if (!fs::exists(directory_path) || !fs::is_directory(directory_path)) {
		std::cerr << "[Error] Could not find directory: " << directory_path << std::endl;
		return;
	}

	for (const auto& fle : fs::directory_iterator(directory_path)) {
		if (fle.is_regular_file() && fle.path().extention() == ".py") {
			std::string udf_name = fle.path().stem().string();
			if (udf_name == "__init__") continue;
			try {
				py::module_::import(udf_name.c_str());
				std::cout << "[OK] Loaded UDF: " << udf_name << std::endl;
			} catch (py::error_already_set& e) {
				std::cerr << "[Error] Failed to load " << udf_name << ": " << e.what() << std::endl;
			}
		}
	}
}

void async_udf_wrapper() {
	
}

Task<void> python_worker(MPSC<DataBatch>& buffer) {
	while (true) {
		DataBatch batch = co_await buffer.pop();
		if (batch.batch_id == -1) {
			std::cout << "[Python worker] Shutting down." << std::endl;
			break;
		}
		std::cout << "[Python worker] Executing batch ID: " << batch.batch_id << std::endl;
	}
}

// command line argument parser ---
struct BenchmarkArgs {
	bool createdb = false;
	bool profiling = false;
	bool print_results = false;

	std::string duckdb_cli = "";
	std::string nthreads = "";
	std::string duckdb_schema = "";
	std::string duckdb_loads = "";
	std::string duckdb_csvs = "";
	std::string duckdb_external = "";
	std::string duckdb_sql = "";
	std::string duckdb_dbfiles = "";
	std::string duckdb_udfs = "";
};

BenchmarkArgs parse_args(int argc, char** argv) {
	BenchmarkArgs config;
	for (int i = 1; i < argc; i++) {
		std::string arg = argv[i];
		if (arg == "--createdb") config.createdb = true;
		else if (arg == "--profiling") config.profiling = true;
		else if (arg == "--print-results") config.print_results = true;
		else if (arg == "--duckdb_cli" && i + 1 < argc) config.duckdbcli = argv[++i];
		else if (arg == "--nthreads" && i + 1 < argc) config.nthreads = argv[++i];
		else if (arg == "--duckdb_schema" && i + 1 < argc) config.duckdb_schema = argv[++i];
		else if (arg == "--duckdb_loads" && i + 1 < argc) config.duckdb_loads = argv[++i];
		else if (arg == "--duckdb_csvs" && i + 1 < argc) config.duckdb_csvs = argv[++i];
		else if (arg == "--duckdb_external" && i + 1 < argc) config.duckdb_external = argv[++i];
		else if (arg == "--duckdb_sql" && i + 1 < argc) config.duckdb_sql = argv[++i];
		else if (arg == "--dudckdb_dbfiles" && i + 1 < argc) config.duck_dbfiles = argv[++i];
		else if (arg == "--duckdb_udfs" && i + 1 < argc) config.duckdb_udfs = argv[++i];
	}

	return config;
}

// main ---------------------------
int main(int argc, char** argv) {
	BenchmarkArgs config = parse_args(argc, argv);

	// Start the interpreter and keep it alive
    py::scoped_interpreter guard{};

	// initialization
	const std::string udf_base = "../UDFBench/engines/duckdb/udfs";
	const std::string udf_scalar = udf_base + "/scalar";
	const std::string udf_table = udf_base + "/table";

	try {
		py::module_ sys = py::module_::import("sys");
		py::list path = sys.attr("path");
		path.append(udf_scalar), path.append(udf_table);

		py::module_::import("pyarrow");
		load_udfs(udf_scalar), load_udfs(udf_table);
		std::cout << "Initialization complete" << std::endl;
	} catch (py::error_already_set& e) {
		std::cerr << "[Error] Python error: " << e.what() << std::endl;
		return 1;
	}

	// pipeline setup
	MPSC<DataBatch> task_buffer;
	auto consumer = python_worker(task_buffer);
	std::cout << "Pipeline Ready" << std::endl;

	// execution

	// teardown/cleanup

	return 0;
};