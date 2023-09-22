#include <Columns/ColumnVector.h>
#include <Core/NamesAndTypes.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>

namespace DB
{

struct free_deleter
{
    template <typename T>
    void operator()(T * p) const
    {
        std::free(const_cast<std::remove_const_t<T> *>(p));
    }
};

static constexpr uint32_t SALT[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
                                     0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

class BlockBloomFilter
{
public:
    BlockBloomFilter() = default;

    explicit BlockBloomFilter(size_t ndv_) { init(ndv_); }
    void init(size_t ndv_)
    {
        assert(ndv_ > 0);
        ndv = ndv_;
        auto tmp_slots = (ndv + ndv_per_slots - 1) / ndv_per_slots;
        auto slots_exp = 64 - __builtin_clzl(tmp_slots - 1);
        slots = 1UL << slots_exp;
        data = std::unique_ptr<UInt8[], free_deleter>(static_cast<UInt8 *>(std::aligned_alloc(bytes_in_slot, slots * bytes_in_slot)));
        std::memset(data.get(), 0, bytes_in_slot * slots);
    }

    void addKey(UInt64 x)
    {
        auto h = DefaultHash<UInt64>()(x);
        addKeyUnhash(h);
    }

    bool probeKey(UInt64 x) const
    {
        auto h = DefaultHash<UInt64>()(x);
        return probeKeyUnhash(h);
    }

    void addKeyUnhash(UInt64 h);
    bool probeKeyUnhash(UInt64 h) const;

    // note: one of the bloom filter will be invalidated
    static BlockBloomFilter intersect(BlockBloomFilter && left, BlockBloomFilter && right);

    void mergeInplace(BlockBloomFilter && bf);

    void deserialize(ReadBuffer & istr);
    void serializeToBuffer(WriteBuffer & ostr);

public:
    static constexpr UInt64 bit_exp = 8; // 256 is a cache line
    static constexpr UInt64 bitmask = (1 << bit_exp) - 1;
    static constexpr UInt64 bits_in_slot = (1 << bit_exp);
    static constexpr UInt64 bytes_in_slot = bits_in_slot / 8;
    static constexpr UInt64 uint32s_in_slot = bits_in_slot >> 5;
    static constexpr UInt64 uint32x4s_in_slot = bits_in_slot >> 7;

    static constexpr UInt64 ndv_per_slots = 32;
    UInt64 ndv;
    UInt64 slots;
    std::unique_ptr<UInt8[], free_deleter> data;
    std::vector<UInt64> hashes;
};
}
