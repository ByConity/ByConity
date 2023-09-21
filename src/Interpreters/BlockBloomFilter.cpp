#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BlockBloomFilter.h>
#include <Interpreters/Set.h>
#include <Common/HashTable/Hash.h>
#include <Common/typeid_cast.h>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#endif


namespace DB
{

#if USE_MULTITARGET_CODE

DECLARE_AVX2_SPECIFIC_CODE(
    static inline __m256i makeMask(const uint32_t hash) noexcept {
        const __m256i ones = _mm256_set1_epi32(1);
        // Odd contants for hashing:
        const __m256i rehash
            = _mm256_setr_epi32(0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U);
        // Load hash into a YMM register, repeated eight times
        __m256i hash_data = _mm256_set1_epi32(hash);
        // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
        // odd constants, then keep the 5 most significant bits from each product.
        hash_data = _mm256_mullo_epi32(rehash, hash_data);
        hash_data = _mm256_srli_epi32(hash_data, 27);
        // Use these 5 bits to shift a single bit to a location in each 32-bit lane
        return _mm256_sllv_epi32(ones, hash_data);
    }

    void addKeyUnhash(UInt64 h, UInt64 slots, void * data_ptr) {
        auto slot_index = (h >> 32) & (slots - 1);
        auto mask = TargetSpecific::AVX2::makeMask(h);
        __m256i * ptr = reinterpret_cast<__m256i *>(data_ptr) + slot_index;
        __builtin_assume_aligned(ptr, BlockBloomFilter::bytes_in_slot);
        __m256i data = _mm256_load_si256(ptr);
        __m256i res = _mm256_or_si256(data, mask);
        _mm256_store_si256(ptr, res);
    }

    bool probeKeyUnhash(UInt64 h, UInt64 slots, void * data_ptr) {
        auto slot_index = (h >> 32) & (slots - 1);
        auto mask = TargetSpecific::AVX2::makeMask(h);
        auto ptr = reinterpret_cast<__m256i *>(data_ptr) + slot_index;
        __builtin_assume_aligned(ptr, BlockBloomFilter::bytes_in_slot);
        const __m256i data = _mm256_load_si256(ptr);
        auto res = _mm256_testc_si256(data, mask);
        return res;
    })

#endif


#if defined(__aarch64__) && defined(__ARM_NEON)

static inline void makeMask(uint32_t key, uint32x4_t* masks) noexcept {
    uint32x4_t hash_data_1 = vdupq_n_u32(key);
    uint32x4_t hash_data_2 = vdupq_n_u32(key);
    uint32x4_t rehash_1 = vld1q_u32(&SALT[0]);
    uint32x4_t rehash_2 = vld1q_u32(&SALT[4]);
    hash_data_1 = vmulq_u32(rehash_1, hash_data_1);
    hash_data_2 = vmulq_u32(rehash_2, hash_data_2);
    hash_data_1 = vshrq_n_u32(hash_data_1, 27);
    hash_data_2 = vshrq_n_u32(hash_data_2, 27);
    const uint32x4_t ones = vdupq_n_u32(1);
    masks[0] = vshlq_u32(ones, reinterpret_cast<int32x4_t>(hash_data_1));
    masks[1] = vshlq_u32(ones, reinterpret_cast<int32x4_t>(hash_data_2));
}

void BlockBloomFilter::addKeyUnhash(UInt64 h)
{
    auto slot_index = (h >> 32) & (slots - 1);
    uint32x4_t masks[uint32x4s_in_slot];

    makeMask(h >> 32, masks);

    uint32_t* ptr = reinterpret_cast<uint32_t*>(data.get()) + slot_index * uint32s_in_slot;
    __builtin_assume_aligned(ptr, bytes_in_slot);

    uint32x4_t data = vld1q_u32(ptr);
    uint32x4_t res = vorrq_u32(data, masks[0]);
    vst1q_u32(ptr, res);


    data = vld1q_u32(ptr + 4);
    res = vorrq_u32(data, masks[1]);
    vst1q_u32(ptr + 4, res);

    if (save_hash)
        hashes.push_back(h);
}

bool BlockBloomFilter::probeKeyUnhash(UInt64 h) const
{
    auto slot_index = (h >> 32) & (slots - 1);
    uint32_t* cache_line = reinterpret_cast<uint32_t*>(data.get());
    uint32x4_t masks[uint32x4s_in_slot];

    uint32x4_t line_1 = vld1q_u32(&cache_line[slot_index * uint32s_in_slot]);
    uint32x4_t line_2 = vld1q_u32(&cache_line[slot_index * uint32s_in_slot + 4]);

    makeMask(h >> 32, masks);
    uint32x4_t out_1 = vbicq_u32(masks[0], line_1);
    uint32x4_t out_2 = vbicq_u32(masks[1], line_2);
    out_1 = vorrq_u32(out_1, out_2);
    uint32x2_t low_1 = vget_low_u32(out_1);
    uint32x2_t high_1 = vget_high_u32(out_1);
    low_1 = vorr_u32(low_1, high_1);
    uint32_t res = vget_lane_u32(low_1, 0) | vget_lane_u32(low_1, 1);
    return !(res);
}

#else 


// MakeMask for scalar version:

static inline void makeMask(uint32_t key, uint32_t* masks) {
    for (int i = 0; i < BlockBloomFilter::uint32s_in_slot; ++i)
    {
        // add some salt to key
        masks[i] = key * SALT[i];
        // masks[i] mod 32
        masks[i] = masks[i] >> 27;
        // set the masks[i]-th bit
        masks[i] = 0x1 << masks[i];
    }
}

void BlockBloomFilter::addKeyUnhash(UInt64 h)
{

#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX2)) {
        if (save_hash)
            hashes.push_back(h);
        return TargetSpecific::AVX2::addKeyUnhash(h, slots, this->data.get());
    }
#endif

    // scalar version
    auto slot_index = (h >> 32) & (slots - 1);
    uint32_t masks[uint32s_in_slot];
    makeMask(h >> 32, masks);
    uint32_t* cache_line = reinterpret_cast<uint32_t*>(data.get());
    for (int i = 0; i < uint32s_in_slot; ++i) {
        cache_line[slot_index * uint32s_in_slot + i] |= masks[i];
    }
    if (save_hash)
        hashes.push_back(h);
}

bool BlockBloomFilter::probeKeyUnhash(UInt64 h) const
{

#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX2)) {
        return TargetSpecific::AVX2::probeKeyUnhash(h, slots, this->data.get());
    }
#endif

    // scalar version
    auto slot_index = (h >> 32) & (slots - 1);
    uint32_t masks[uint32s_in_slot];
    makeMask(h >> 32, masks);
    uint32_t* cache_line = reinterpret_cast<uint32_t*>(data.get());
    for (int i = 0; i < uint32s_in_slot; ++i) {
        if ((cache_line[slot_index * uint32s_in_slot + i] & masks[i]) == 0) {
            return false;
        }
    }
    return true;
}

#endif
// note: one of the bloom filter will be invalidated
BlockBloomFilter BlockBloomFilter::intersect(BlockBloomFilter && left_, BlockBloomFilter && right_)
{
    BlockBloomFilter * left_ptr = &left_;
    BlockBloomFilter * right_ptr = &right_;
    if (left_.slots < right_.slots)
    {
        std::swap(left_ptr, right_ptr);
    }
    // left has larger slots than right

    auto * __restrict left_data = left_ptr->data.get();
    auto * __restrict right_data = right_ptr->data.get();

    auto left_index = 0;
    do
    {
        for (auto i = 0; i < right_ptr->slots * bytes_in_slot; ++i)
        {
            left_data[left_index] &= right_data[i];
        }
    } while (left_index >= left_ptr->slots * bytes_in_slot);

    return std::move(*left_ptr);
}

void BlockBloomFilter::deserialize(ReadBuffer & istr)
{
    readBinary(ndv, istr);
    init(ndv);
    istr.read(reinterpret_cast<char *>(data.get()), bytes_in_slot * slots);
    readBinary(save_hash, istr);
    if (save_hash)
        readVectorBinary(hashes, istr);
}

void BlockBloomFilter::serializeToBuffer(WriteBuffer & ostr)
{
    writeBinary(ndv, ostr);
    ostr.write(reinterpret_cast<const char *>(data.get()), bytes_in_slot * slots);
    writeBinary(save_hash, ostr);
    if (save_hash)
        writeVectorBinary(hashes, ostr);
}

void BlockBloomFilter::mergeInplace(const BlockBloomFilter & bf)
{
    if (this->slots != bf.slots)
    {
        if (!isPowerOf2(this->slots) || !isPowerOf2(bf.slots))
            throw Exception("Cannot merge bloom filters with none power of two bit size", ErrorCodes::LOGICAL_ERROR);

        if (this->slots < bf.slots)
        {
            std::unique_ptr<UInt8[], free_deleter> new_data =
                std::unique_ptr<UInt8[], free_deleter>(static_cast<UInt8 *>(std::aligned_alloc(bytes_in_slot, bf.slots * bytes_in_slot)));

            size_t start = 0;
            size_t total = bf.slots * bytes_in_slot;
            do
            {
                memcpy(new_data.get() + start, data.get(), bytes_in_slot * slots);
                start += bytes_in_slot * slots;
            } while(start < total);
            data = std::move(new_data);
            slots = bf.slots;
        }
        else
        {
            size_t total = bytes_in_slot * slots;
            size_t step  = bytes_in_slot * bf.slots;
            for (size_t start = 0; start < total; start += step)
            {
                for (size_t i = 0; i < bytes_in_slot * bf.slots; i++)
                    this->data[start + i] |= bf.data[i];
            }
            return ;
        }
    }

    for (size_t i = 0; i < bytes_in_slot * slots; i++)
        this->data[i] |= bf.data[i];
}

}
