//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstdint>

#ifndef BF_RESTRICT
#if defined(_MSC_VER)
#define BF_RESTRICT __restrict
#elif defined(__GNUC__) || defined(__clang__)
#define BF_RESTRICT __restrict__
#else
// Fallback: just return the pointer as-is
#define BF_RESTRICT
#endif
#endif

namespace duckdb {

static constexpr const uint32_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr const uint32_t MIN_NUM_BITS_PER_KEY = 16;
static constexpr const uint32_t MIN_NUM_BITS = 512;
static constexpr const uint32_t LOG_SECTOR_SIZE = 5;
static constexpr const int32_t SIMD_BATCH_SIZE = 16;

static uint32_t CeilPowerOfTwo(uint32_t n) {
	if (n <= 1) {
		return 1;
	}
	n--;
	n |= (n >> 1);
	n |= (n >> 2);
	n |= (n >> 4);
	n |= (n >> 8);
	n |= (n >> 16);
	return n + 1;
}

static Vector HashColumns(DataChunk &chunk, idx_t bloom_probe_idx) {
	auto count = chunk.size();
	Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(chunk.data[bloom_probe_idx], hashes, count);

	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		hashes.Flatten(count);
	}
	return hashes;
}

class BloomFilter {
public:
	BloomFilter() = default;

	void Initialize(ClientContext &context_p, uint32_t est_num_rows) {
		context = &context_p;
		buffer_manager = &BufferManager::GetBufferManager(*context);

		uint32_t min_bits = std::max<uint32_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
		num_sectors = std::min(CeilPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
		num_sectors_log = static_cast<uint32_t>(std::log2(num_sectors));

		buf_ = buffer_manager->GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
		// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
		blocks = reinterpret_cast<uint32_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
		std::fill_n(blocks, num_sectors, 0);
	}

	ClientContext *context = nullptr;
	BufferManager *buffer_manager = nullptr;
	uint32_t num_sectors = 0;
	uint32_t num_sectors_log = 0;
	uint32_t *blocks = nullptr;

private:
	// key_lo |5:bit3|5:bit2|5:bit1|  13:block    |4:sector1 | bit layout (32:total)
	// key_hi |5:bit4|5:bit3|5:bit2|5:bit1|9:block|3:sector2 | bit layout (32:total)
	inline uint32_t GetMask1(uint32_t key_lo) const {
		// 3 bits in key_lo
		return (1u << ((key_lo >> 17) & 31)) | (1u << ((key_lo >> 22) & 31)) | (1u << ((key_lo >> 27) & 31));
	}
	inline uint32_t GetMask2(uint32_t key_hi) const {
		// 4 bits in key_hi
		return (1u << ((key_hi >> 12) & 31)) | (1u << ((key_hi >> 17) & 31)) | (1u << ((key_hi >> 22) & 31)) |
		       (1u << ((key_hi >> 27) & 31));
	}

	inline uint32_t GetSector1(uint32_t key_lo, uint32_t key_hi) const {
		// block: 13 bits in key_lo and 9 bits in key_hi
		// sector 1: 4 bits in key_lo
		return ((key_lo & ((1 << 17) - 1)) + ((key_hi << 14) & (((1 << 9) - 1) << 17))) & (num_sectors - 1);
	}
	inline uint32_t GetSector2(uint32_t key_hi, uint32_t block1) const {
		// sector 2: 3 bits in key_hi
		return block1 ^ (8 + (key_hi & 7));
	}

	inline void InsertOne(uint32_t key_lo, uint32_t key_hi, uint32_t *BF_RESTRICT bf) const {
		uint32_t sector1 = GetSector1(key_lo, key_hi);
		uint32_t mask1 = GetMask1(key_lo);
		uint32_t sector2 = GetSector2(key_hi, sector1);
		uint32_t mask2 = GetMask2(key_hi);
		bf[sector1] |= mask1;
		bf[sector2] |= mask2;
	}
	inline bool LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *BF_RESTRICT bf) const {
		uint32_t sector1 = GetSector1(key_lo, key_hi);
		uint32_t mask1 = GetMask1(key_lo);
		uint32_t sector2 = GetSector2(key_hi, sector1);
		uint32_t mask2 = GetMask2(key_hi);
		return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
	}

private:
	int BloomFilterLookup(int num, const uint64_t *BF_RESTRICT key64, const uint32_t *BF_RESTRICT bf,
	                      uint32_t *BF_RESTRICT out) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
		for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				out[i + j] = ((bf[block1[j]] & mask1[j]) == mask1[j]) & ((bf[block2[j]] & mask2[j]) == mask2[j]);
			}
		}

		// unaligned tail
		for (int i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			out[i] = LookupOne(key[i + i], key[i + i + 1], bf);
		}
		return num;
	}

	void BloomFilterInsert(int num, const uint64_t *BF_RESTRICT key64, uint32_t *BF_RESTRICT bf) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
		for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				bf[block1[j]] |= mask1[j];
				bf[block2[j]] |= mask2[j];
			}
		}

		// unaligned tail
		for (int i = num & ~(SIMD_BATCH_SIZE - 1); i < num; i++) {
			InsertOne(key[i + i], key[i + i + 1], bf);
		}
	}

	AllocatedData buf_;

public:
	void Lookup(DataChunk &input, idx_t bloom_probe_idx, SelectionVector &sel, vector<uint32_t> &lookup_results,
	            DataChunk &output) const {
		int count = static_cast<int>(input.size());
		Vector hashes = HashColumns(input, bloom_probe_idx);
		BloomFilterLookup(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks, lookup_results.data());

		idx_t result_count = 0;
		for (size_t i = 0; i < input.size(); i++) {
			sel.set_index(result_count, i);
			result_count += lookup_results[i];
		}
		if (result_count == input.size()) {
			// nothing was filtered: skip adding any selection vectors
			output.Reference(input);
		} else {
			output.Slice(input, sel, result_count);
		}
	}

	void Insert(DataChunk &input) {
		int count = static_cast<int>(input.size());
		Vector hashes = HashColumns(input, 0);
		BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks);
	}

	BloomFilter &operator|=(const BloomFilter &other) {
		// Perform bitwise OR across all sectors
		for (uint32_t i = 0; i < num_sectors; i++) {
			blocks[i] |= other.blocks[i];
		}
		return *this;
	}
};

} // namespace duckdb
