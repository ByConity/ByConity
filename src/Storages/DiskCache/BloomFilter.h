#pragma once

#include <istream>
#include <memory>
#include <ostream>
#include <utility>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message_lite.h>

#include <Common/Exception.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB::HybridCache
{
// Organize an array of BFs in a byte array. User does BF operations referencing BF with
// an index. It solves problem of lots of small BFs: allocated one-by-one BFs
// have large overhead. By default, the bloom filter is initialized to
// indicate that it is empty and couldExist would return false.
//
// Thread safe if user guards operations to an idx.
class BloomFilter
{
public:
    BloomFilter() = default;

    BloomFilter(UInt32 num_filters_, UInt32 num_hashes_, size_t hash_table_bit_size_);

    static BloomFilter makeBloomFilter(UInt32 num_filters, size_t element_count, double pf_prob);

    BloomFilter(const BloomFilter &) = delete;
    BloomFilter & operator=(const BloomFilter &) = delete;

    BloomFilter(BloomFilter && other) noexcept
        : filters_count(other.filters_count)
        , hash_table_bit_size(other.hash_table_bit_size)
        , filter_byte_size(other.filter_byte_size)
        , seeds(std::exchange(other.seeds, {}))
        , bits(std::move(other.bits))
    {
    }

    BloomFilter & operator=(BloomFilter && other)
    {
        if (this != &other)
        {
            this->~BloomFilter();
            new (this) BloomFilter(std::move(other));
        }
        return *this;
    }

    void set(UInt32 index, UInt64 key);
    bool couldExist(UInt32 index, UInt64 key) const;

    void clear(UInt32 index);

    void reset();

    UInt32 numFilters() const { return filters_count; }

    UInt32 numHashes() const { return static_cast<UInt32>(seeds.size()); }

    size_t numBitsPerFilter() const { return filter_byte_size * 8ULL; }

    size_t getByteSize() const { return filters_count * filter_byte_size; }

    void persist(google::protobuf::io::CodedOutputStream * stream);
    void recover(google::protobuf::io::CodedInputStream * stream);

private:
    UInt8 * getFilterBytes(UInt32 index) const
    {
        chassert(bits);
        return bits.get() + index * filter_byte_size;
    }

    static constexpr UInt32 kPersistFragmentSize = 1024 * 1024;

    void serializeInternal(google::protobuf::io::CodedOutputStream * stream, UInt64 fragment_size);
    void deserializeInternal(google::protobuf::io::CodedInputStream * stream, UInt64 fragment_size);

    const UInt32 filters_count{};
    const size_t hash_table_bit_size{};
    const size_t filter_byte_size{};
    std::vector<UInt64> seeds;
    std::unique_ptr<UInt8[]> bits;
};
}
