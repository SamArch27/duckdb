#pragma once

#include <bit>
#include <cstdint>
#include <memory>
#include <random>
#include <algorithm>
#include <type_traits>
#include <utility>

namespace acehash {

template <typename Key, typename Enable = void>
class MultiplyAddHasher {};

template <typename Key>
class MultiplyAddHasher<Key, std::enable_if_t<std::is_same_v<Key, int8_t> || std::is_same_v<Key, uint8_t> ||
                                              std::is_same_v<Key, int16_t> || std::is_same_v<Key, uint16_t> ||
                                              std::is_same_v<Key, int32_t> || std::is_same_v<Key, uint32_t>>> {
public:
	MultiplyAddHasher() {
		std::random_device rd;
		std::minstd_rand gen(rd());
		initialize(gen);
	}

	template <typename RandomGenerator>
	explicit MultiplyAddHasher(RandomGenerator &gen) {
		initialize(gen);
	}

	uint32_t operator()(const Key &key) const {
		return (a_ * key + b_) >> 32;
	}

private:
	template <typename RandomGenerator>
	void initialize(RandomGenerator &gen) {
		std::uniform_int_distribution<uint64_t> dis;
		a_ = dis(gen);
		b_ = dis(gen);
	}

	uint64_t a_ {};
	uint64_t b_ {};
};

template <typename Key>
class MultiplyAddHasher<Key, std::enable_if_t<std::is_same_v<Key, int64_t> || std::is_same_v<Key, uint64_t>>> {
public:
	MultiplyAddHasher() = default;

	template <typename RandomGenerator>
	explicit MultiplyAddHasher(RandomGenerator &gen) : hasher_lo_(gen), hasher_hi_(gen) {
	}

	uint32_t operator()(const Key &key) const {
		return hasher_lo_((uint32_t)key) ^ hasher_hi_((uint32_t)(key >> 32));
	}

private:
	MultiplyAddHasher<uint32_t> hasher_lo_;
	MultiplyAddHasher<uint32_t> hasher_hi_;
};

template <typename Item>
class Partition {
public:
	Partition(Item *begin, Item *end, uint32_t offset) : begin_(begin), end_(end), offset_(offset) {
	}

	[[nodiscard]] uint32_t offset() const {
		return offset_;
	}

	const Item &operator[](uint32_t index) const {
		return begin_[index];
	}

	[[nodiscard]] uint32_t length() const {
		return std::distance(begin_, end_);
	}

	[[nodiscard]] Item *begin() const {
		return begin_;
	}

	[[nodiscard]] Item *end() const {
		return end_;
	}

private:
	Item *begin_;
	Item *end_;
	uint32_t offset_;
};

template <typename Item>
class Partitions {
public:
	class Iterator {
	public:
		Partition<Item> operator*() {
			return Partition(&items_[*bounds_], &items_[*(bounds_ + 1)], *bounds_);
		}

		Iterator &operator++() {
			++bounds_;
			return *this;
		}

		friend bool operator!=(const Iterator &a, const Iterator &b) {
			return a.bounds_ != b.bounds_;
		};

	private:
		friend Partitions<Item>;

		Iterator(Item *items, std::vector<uint32_t>::iterator bounds) : items_(items), bounds_(bounds) {
		}

		Item *items_;
		std::vector<uint32_t>::iterator bounds_;
	};

	template <typename ItemIterator, typename Scheduler, typename Function>
	Partitions(ItemIterator items, uint32_t num_items, uint32_t num_parts, const Scheduler &scheduler,
	           const Function &function, bool multi_round = false) {
		*this = std::move(multi_round ? two_round(items, num_items, num_parts, scheduler, function)
		                              : one_round(items, num_items, num_parts, scheduler, function));
	}

	Partition<Item> operator[](uint32_t part_index) const {
		uint32_t begin_index = bounds_[part_index];
		uint32_t end_index = bounds_[part_index + 1];
		return Partition<Item>(items_.begin() + begin_index, items_.begin() + end_index, begin_index);
	}

	Iterator begin() {
		return Iterator(items_.begin(), bounds_.begin());
	}

	Iterator end() {
		return Iterator(items_.begin(), bounds_.end() - 1);
	}

private:
	struct ItemBuffer {
		ItemBuffer() : items(nullptr) {
		}

		explicit ItemBuffer(uint32_t num_items) {
			items = new Item[num_items];
		}

		ItemBuffer(const ItemBuffer &) = delete;

		ItemBuffer &operator=(const ItemBuffer &) = delete;

		ItemBuffer(ItemBuffer &&other) noexcept : items(std::exchange(other.items, nullptr)) {
		}

		ItemBuffer &operator=(ItemBuffer &&other) noexcept {
			if (this != &other) {
				delete[] items;
				items = std::exchange(other.items, nullptr);
			}
			return *this;
		}

		~ItemBuffer() {
			delete[] items;
		}

		Item &operator[](uint32_t item_index) {
			return items[item_index];
		}

		[[nodiscard]] Item *begin() const {
			return items;
		}

		[[nodiscard]] Item *end() const {
			return items;
		}

		Item *items;
	};

	template <typename ItemIterator, typename Scheduler, typename Function>
	static Partitions<Item> one_round(ItemIterator src_items, uint32_t num_items, uint32_t num_parts,
	                                  const Scheduler &scheduler, const Function &function) {
		constexpr size_t num_items_per_chunk = uint32_t(1) << 18;
		constexpr size_t min_num_chunks = 1;
		constexpr size_t max_num_chunks = 1000;

		size_t num_chunks = num_items / num_items_per_chunk;
		num_chunks = std::max(num_chunks, min_num_chunks);
		num_chunks = std::min(num_chunks, max_num_chunks);

		ItemBuffer dst_items(num_items);
		std::vector<uint32_t> bounds(num_chunks * num_parts);

		scheduler.map(size_t(0), num_chunks, [&](size_t chunk_index) {
			ItemIterator chunk_begin = src_items + chunk_index * num_items_per_chunk;
			ItemIterator chunk_end =
			    chunk_index < num_chunks - 1 ? chunk_begin + num_items_per_chunk : src_items + num_items;

			for (ItemIterator it = chunk_begin; it != chunk_end; ++it) {
				++bounds[chunk_index * num_parts + function(*it)];
			}
		});

		uint32_t count = 0;
		for (uint32_t part_index = 0; part_index < num_parts; ++part_index) {
			for (size_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
				auto &offset = bounds[chunk_index * num_parts + part_index];
				count += offset;
				offset = count;
			}
		}

		scheduler.map(size_t(0), num_chunks, [&](size_t chunk_index) {
			ItemIterator chunk_begin = src_items + chunk_index * num_items_per_chunk;
			ItemIterator chunk_end =
			    chunk_index < num_chunks - 1 ? chunk_begin + num_items_per_chunk : src_items + num_items;

			for (ItemIterator it = chunk_begin; it != chunk_end; ++it) {
				dst_items[--bounds[chunk_index * num_parts + function(*it)]] = *it;
			}
		});

		bounds.resize(num_parts + 1);
		bounds.back() = num_items;

		return Partitions(std::move(dst_items), std::move(bounds));
	}

	template <typename ItemIterator, typename Scheduler, typename Function>
	static Partitions<Item> two_round(ItemIterator src_items, uint32_t num_items, uint32_t num_parts,
	                                  const Scheduler &scheduler, const Function &function) {
		uint32_t num_parts_r1 = uint32_t(1) << 15;
		if (num_parts <= num_parts_r1) {
			return one_round(src_items, num_items, num_parts, scheduler, function);
		}

		uint32_t num_parts_r2 = num_parts;

		uint32_t shift = 0;
		while (num_parts_r2 >> shift > num_parts_r1) {
			++shift;
		}

		uint32_t num_parts_inner = uint32_t(1) << shift;
		uint32_t num_parts_outer = num_parts_r2 / num_parts_inner + (num_parts_r2 % num_parts_inner != 0);

		Partitions<Item> parts_outer = one_round(src_items, num_items, num_parts_outer, scheduler,
		                                         [&](const Item &item) { return function(item) >> shift; });

		ItemBuffer dst_items(num_items);
		std::vector<uint32_t> bounds(num_parts_r2);

		scheduler.map(uint32_t(0), num_parts_outer, [&](uint32_t part_index_outer) {
			Partition part_outer = parts_outer[part_index_outer];

			for (const Item &item : part_outer) {
				++bounds[function(item)];
			}

			auto bounds_begin = bounds.begin() + part_index_outer * num_parts_inner;
			auto bounds_end = part_index_outer + 1 < num_parts_outer ? bounds_begin + num_parts_inner : bounds.end();

			std::partial_sum(bounds_begin, bounds_end, bounds_begin);

			std::for_each(bounds_begin, bounds_end, [&](uint32_t &offset) { offset += part_outer.offset(); });

			for (const Item &item : part_outer) {
				dst_items[--bounds[function(item)]] = item;
			}
		});

		bounds.resize(num_parts_r2 + 1);
		bounds.back() = num_items;

		return Partitions(std::move(dst_items), std::move(bounds));
	}

	Partitions(ItemBuffer items, std::vector<uint32_t> bounds) : items_(std::move(items)), bounds_(std::move(bounds)) {
	}

	ItemBuffer items_;
	std::vector<uint32_t> bounds_;
};

class IndexGenerator {
public:
	explicit IndexGenerator(uint32_t index = 0) : index_(index) {
	}

	uint32_t operator*() const {
		return index_;
	}

	IndexGenerator &operator++() {
		index_++;
		return *this;
	}

	IndexGenerator operator+(uint32_t offset) const {
		return IndexGenerator(index_ + offset);
	}

	friend bool operator!=(const IndexGenerator &a, const IndexGenerator &b) {
		return a.index_ != b.index_;
	};

private:
	uint32_t index_;
};

template <typename Mutex>
class Occupied {
public:
	explicit Occupied(uint32_t num_slots) {
		uint32_t num_lines = num_slots / 16 + (num_slots % 16 != 0);
		lines_ = std::vector<uint16_t>(num_lines);
		locks_ = std::vector<Mutex>(num_lines);

		if (num_slots % 16 != 0) {
			lines_.back() |= UINT16_MAX << num_slots % 16;
		}
	}

	uint8_t try_lines(std::vector<uint32_t> &line_indices) {
		if (!sort_and_verify_distinct(line_indices)) {
			return UINT8_MAX;
		}

		for (uint32_t line_index : line_indices) {
			locks_[line_index].lock();
		}

		uint8_t result = try_lines_distinct(line_indices);

		for (uint32_t line_index : line_indices) {
			locks_[line_index].unlock();
		}

		return result;
	}

	bool try_slots(std::vector<uint32_t> &slot_indices) {
		if (!sort_and_verify_distinct(slot_indices)) {
			return false;
		}

		auto process_locks = [&](bool flag) {
			for (auto it = slot_indices.begin(); it != slot_indices.end(); ++it) {
				uint32_t line_index = *it / 16;

				if (it != slot_indices.begin()) {
					if (line_index == *(it - 1) / 16) {
						continue;
					}
				}

				if (flag) {
					locks_[line_index].lock();
				} else {
					locks_[line_index].unlock();
				}
			}
		};

		process_locks(true);

		bool result = try_slots_distinct(slot_indices);

		process_locks(false);

		return result;
	}

	std::vector<uint32_t> get_unoccupied_slot_indices(uint32_t num_slot_indices) {
		std::vector<uint32_t> slot_indices(num_slot_indices);

		auto it = slot_indices.begin();

		for (uint32_t line_index = 0; line_index < lines_.size(); ++line_index) {
			if (it == slot_indices.end()) {
				break;
			}

			uint16_t line = ~lines_[line_index];
			while (it != slot_indices.end() && line != 0) {
				uint32_t n = std::countr_zero(line);
				*it++ = line_index * 16 + n;
				line ^= uint16_t(1) << n;
			}
		}

		return slot_indices;
	}

private:
	static bool sort_and_verify_distinct(std::vector<uint32_t> &indices) {
		std::sort(indices.begin(), indices.end());
		return std::adjacent_find(indices.begin(), indices.end()) == indices.end();
	}

	bool get_slot(uint32_t slot_index) {
		return lines_[slot_index / 16] >> slot_index % 16 & 1;
	}

	void set_slot(uint32_t slot_index) {
		lines_[slot_index / 16] |= uint16_t(1) << slot_index % 16;
	}

	bool try_slots_distinct(std::vector<uint32_t> &slot_indices) {
		for (uint32_t slot_index : slot_indices) {
			if (get_slot(slot_index)) {
				return false;
			}
		}

		for (uint32_t slot_index : slot_indices) {
			set_slot(slot_index);
		}

		return true;
	}

	uint8_t try_lines_distinct(const std::vector<uint32_t> &line_indices) {
		uint16_t line = 0;
		for (uint32_t line_index : line_indices) {
			line |= lines_[line_index];
		}

		if (line == UINT16_MAX) {
			return UINT8_MAX;
		}

		uint8_t result = std::countr_one(line);

		for (uint32_t line_index : line_indices) {
			set_slot(line_index * 16 + result);
		}

		return result;
	}

	std::vector<uint16_t> lines_;
	std::vector<Mutex> locks_;
};

struct SerialScheduler {
	template <typename Item>
	class ThreadLocal {
	public:
		Item *begin() {
			return &item_;
		}

		Item *end() {
			return &item_ + 1;
		}

		Item &local() {
			return item_;
		}

	private:
		Item item_;
	};

	struct Mutex {
		void lock() {
		}

		void unlock() {
		}
	};

	template <typename Index, typename Function>
	void map(Index begin, Index end, const Function &function) const {
		for (Index index = begin; index != end; ++index) {
			function(index);
		}
	}
};

template <typename Scheduler = SerialScheduler, typename Generator = std::mt19937>
struct Config {
	double lambda = 2.5;
	double alpha = 1.0;
	Generator generator = Generator(0);
	Scheduler scheduler = {};
};

template <typename Key, typename Hasher, bool multi_round_partition, bool multi_slot_search>
class Node {
public:
	Node() : num_seeds_(0), num_slots_(0), num_lines_(0) {
	}

	template <typename KeyIterator, typename Scheduler = SerialScheduler, typename Generator = std::mt19937>
	Node(KeyIterator begin, KeyIterator end, Config<Scheduler, Generator> config = {}, double *fraction_f1 = nullptr) {
		hasher_1_ = Hasher(config.generator);
		hasher_2_ = Hasher(config.generator);
		hasher_3_ = Hasher(config.generator);

		uint32_t num_keys = std::distance(begin, end);

		num_seeds_ = std::ceil(num_keys / config.lambda);
		num_slots_ = config.alpha == 1.0 ? num_keys : std::ceil(num_keys / config.alpha);
		num_lines_ = num_slots_ / 16 + (num_slots_ % 16 != 0);

		Partitions<Key> buckets(
		    begin, num_keys, num_seeds_, config.scheduler, [&](const Key &key) { return get_seed_index(key); },
		    multi_round_partition);

		uint32_t num_groups = 64;
		Partitions<uint32_t> groups(IndexGenerator(), num_seeds_, num_groups, config.scheduler,
		                            [&](uint32_t seed_index) {
			                            uint32_t bucket_length = buckets[seed_index].length();
			                            return (num_groups - 1) - std::min(bucket_length, (num_groups - 1));
		                            });

		seeds_.resize(num_seeds_);

		Occupied<typename Scheduler::Mutex> occupied(num_slots_);

		typename Scheduler::template ThreadLocal<std::vector<uint32_t>> indices;
		typename Scheduler::template ThreadLocal<std::vector<Key>> residual;

		for (const Partition<uint32_t> &group : groups) {
			config.scheduler.map(uint32_t(0), group.length(), [&](uint32_t index) {
				uint32_t seed_index = group[index];

				const Partition<Key> &bucket = buckets[seed_index];
				uint8_t &seed = seeds_[seed_index];

				std::vector<uint32_t> &local_indices = indices.local();
				std::vector<Key> &local_residual = residual.local();

				local_indices.resize(bucket.length());

				if constexpr (multi_slot_search) {
					seed = UINT8_MAX;
					for (uint8_t seed_1 = 0; seed_1 < 15; ++seed_1) {
						std::transform(bucket.begin(), bucket.end(), local_indices.begin(),
						               [&](const Key &key) { return get_line_index(key, seed_1); });

						uint8_t seed_2 = occupied.try_lines(local_indices);

						if (seed_2 != UINT8_MAX) {
							seed = seed_1 * 16 + seed_2;
							break;
						}
					}
				} else {
					for (seed = 0; seed < UINT8_MAX; ++seed) {
						std::transform(bucket.begin(), bucket.end(), local_indices.begin(),
						               [&](const Key &key) { return get_slot_index(key, seed); });

						if (occupied.try_slots(local_indices)) {
							break;
						}
					}
				}

				if (seed == UINT8_MAX) {
					local_residual.insert(local_residual.end(), bucket.begin(), bucket.end());
				}
			});
		}

		if (fraction_f1) {
			uint32_t num_f1 = 0;
			for (uint32_t seed_index = 0; seed_index < num_seeds_; ++seed_index) {
				if (seeds_[seed_index] != UINT8_MAX) {
					num_f1 += buckets[seed_index].length();
				}
			}

			*fraction_f1 = (double)num_f1 / num_keys;
		}

		std::vector<Key> next_keys;
		for (const std::vector<Key> &local_residual : residual) {
			next_keys.insert(next_keys.begin(), local_residual.begin(), local_residual.end());
		}

		if (!next_keys.empty()) {
			offsets_ = occupied.get_unoccupied_slot_indices(next_keys.size());

			config.lambda = 2.0;
			config.alpha = 1.0;
			next_ = std::make_unique<Node<Key, Hasher, multi_round_partition, false>>(
			    next_keys.begin(), next_keys.end(), std::move(config));
		}
	}

	uint32_t operator()(const Key &key) const {
		uint8_t seed = seeds_[get_seed_index(key)];

		if (seed == UINT8_MAX) {
			return offsets_[(*next_)(key)];
		}

		if constexpr (multi_slot_search) {
			return get_line_index(key, seed / 16) * 16 + seed % 16;
		}

		return get_slot_index(key, seed);
	}

	void operator()(size_t n, const Key *keys, uint32_t *slot_indices) const {
		constexpr size_t batch_length = 128;

		size_t bound = n / batch_length * batch_length;

		for (size_t i = 0; i < bound; i += batch_length) {
			uint32_t seed_indices[batch_length];
			for (size_t j = 0; j < batch_length; ++j) {
				seed_indices[j] = get_seed_index(keys[i + j]);
			}

			uint8_t seeds[batch_length];
			for (size_t j = 0; j < batch_length; ++j) {
				seeds[j] = seeds_[seed_indices[j]];
			}

			if constexpr (multi_slot_search) {
				uint32_t line_indices[batch_length];
				for (size_t j = 0; j < batch_length; ++j) {
					line_indices[j] = get_line_index(keys[i + j], seeds[j] / 16);
				}

				for (size_t j = 0; j < batch_length; ++j) {
					slot_indices[i + j] = line_indices[j] * 16 + seeds[j] % 16;
				}
			} else {
				for (size_t j = 0; j < batch_length; ++j) {
					slot_indices[i + j] = get_slot_index(keys[i + j], seeds[j]);
				}
			}

			for (size_t j = 0; j < batch_length; ++j) {
				if (seeds[j] == UINT8_MAX) {
					slot_indices[i + j] = offsets_[(*next_)(keys[i + j])];
				}
			}
		}

		for (size_t i = bound; i < n; ++i) {
			slot_indices[i] = (*this)(keys[i]);
		}
	}

	[[nodiscard]] uint32_t num_slots() const {
		return num_slots_;
	}

	[[nodiscard]] uint32_t num_bits() const {
		uint32_t num_bits = 8 * seeds_.size() + 32 * offsets_.size();
		if (next_) {
			num_bits += next_->num_bits();
		}

		return num_bits;
	}

private:
	static uint32_t reduce(uint32_t a, uint32_t b) {
		return (uint64_t)a * b >> 32;
	}

	uint32_t get_seed_index(const Key &key) const {
		return reduce(hasher_1_(key), num_seeds_);
	}

	uint32_t get_slot_index(const Key &key, uint8_t seed) const {
		return reduce(hasher_2_(key) * seed + hasher_3_(key), num_slots_);
	}

	uint32_t get_line_index(const Key &key, uint8_t seed) const {
		return reduce(hasher_2_(key) * seed + hasher_3_(key), num_lines_);
	}

	Hasher hasher_1_;
	Hasher hasher_2_;
	Hasher hasher_3_;
	uint32_t num_seeds_;
	uint32_t num_slots_;
	uint32_t num_lines_;
	std::vector<uint8_t> seeds_;
	std::vector<uint32_t> offsets_;
	std::unique_ptr<Node<Key, Hasher, multi_round_partition, false>> next_;
};

template <typename Key, typename Hasher = MultiplyAddHasher<Key>, bool multi_round_partition = true,
          bool multi_slot_search = true>
using AceHash = Node<Key, Hasher, multi_round_partition, multi_slot_search>;

template <typename Key, bool multi_round_partition, bool multi_slot_search, bool multi_key_evaluate,
          typename Scheduler = acehash::SerialScheduler>
class AceHashFunction {
public:
	AceHashFunction() = default;

	AceHashFunction(size_t n, const Key *keys, double lambda, double alpha, double *fraction_f1 = nullptr)
	    : lambda_(lambda), alpha_(alpha),
	      function_(keys, keys + n, acehash::Config<Scheduler> {.lambda = lambda_, .alpha = alpha_}, fraction_f1) {
	}

	uint32_t operator()(const Key &key) const {
		return function_(key);
	}

	void operator()(size_t n, const Key *keys, uint32_t *indices) const {
		if (multi_key_evaluate) {
			function_(n, keys, indices);
		} else {
			for (size_t i = 0; i < n; ++i) {
				indices[i] = (*this)(keys[i]);
			}
		}
	}

	template <typename Callback>
	void evaluate(size_t n, const Key *keys, const Callback &callback) {
		constexpr size_t batch_length = 128;

		size_t i = 0;

		for (; i < n / batch_length * batch_length; i += batch_length) {
			uint32_t indices[batch_length];
			function_(batch_length, &keys[i], indices);

			for (size_t j = 0; j < batch_length; ++j) {
				callback(i + j, indices[j]);
			}
		}

		for (; i < n; ++i) {
			callback(i, function_(keys[i]));
		}
	}

	[[nodiscard]] size_t num_slots() const {
		return function_.num_slots();
	}

	[[nodiscard]] size_t num_bits() const {
		return function_.num_bits();
	}

	[[nodiscard]] std::string algorithm() const {
		std::ostringstream out;
		out << "AceHash;" << multi_round_partition << ';' << multi_slot_search << ';' << multi_key_evaluate << ';'
		    << !std::is_same_v<Scheduler, acehash::SerialScheduler>;
		return out.str();
	}

	[[nodiscard]] std::string parameters() const {
		std::ostringstream out;
		out << lambda_ << ';' << alpha_;
		return out.str();
	}

private:
	double lambda_ {};
	double alpha_ {};
	acehash::AceHash<Key, acehash::MultiplyAddHasher<Key>, multi_round_partition, multi_slot_search> function_;
};

template <typename Key>
using AceHashFunctionV1 = AceHashFunction<Key, false, false, false, acehash::SerialScheduler>;

template <typename Key>
using AceHashFunctionV2 = AceHashFunction<Key, true, false, false, acehash::SerialScheduler>;

template <typename Key>
using AceHashFunctionV3 = AceHashFunction<Key, true, true, false, acehash::SerialScheduler>;

template <typename Key>
using AceHashFunctionV4 = AceHashFunction<Key, true, true, true, acehash::SerialScheduler>;

template <typename Function, typename Key, typename Value>
class PerfectMap {
public:
	PerfectMap() = default;

	template <typename... Args>
	PerfectMap(size_t n, const Key *keys, const Value *values, Args... args)
	    : function_(n, keys, args...), values_(function_.num_slots()) {
		for (size_t i = 0; i < n; ++i) {
			values_[function_(keys[i])] = values[i];
		}
	}

	template <typename Callback>
	void operate(size_t n, const Key *keys, const Callback &callback) {
		constexpr size_t batch_length = 128;

		size_t i = 0;

		for (; i < n / batch_length * batch_length; i += batch_length) {
			uint32_t indices[batch_length];
			function_(batch_length, &keys[i], indices);

			for (size_t j = 0; j < batch_length; ++j) {
				callback(i + j, values_[indices[j]]);
			}
		}

		for (; i < n; ++i) {
			callback(i, values_[function_(keys[i])]);
		}
	}

	void retrieve(size_t n, const Key *keys, Value *values) {
		operate(n, keys, [&](size_t i, const Value &value) { values[i] = value; });
	}

	Value &retrieve(const Key &key) {
		return values_[function_(key)];
	}

	[[nodiscard]] size_t num_bits() const {
		return function_.num_bits() + 8 * sizeof(Value) * values_.size();
	}

	[[nodiscard]] std::string algorithm() const {
		return function_.algorithm();
	}

	[[nodiscard]] std::string parameters() const {
		return function_.parameters();
	}

private:
	Function function_;
	std::vector<Value> values_;
};

template <typename Key, typename Value>
using AceHashMapV1 = PerfectMap<AceHashFunctionV1<Key>, Key, Value>;

template <typename Key, typename Value>
using AceHashMapV2 = PerfectMap<AceHashFunctionV2<Key>, Key, Value>;

template <typename Key, typename Value>
using AceHashMapV3 = PerfectMap<AceHashFunctionV3<Key>, Key, Value>;

template <typename Key, typename Value>
using AceHashMapV4 = PerfectMap<AceHashFunctionV4<Key>, Key, Value>;

} // namespace acehash
