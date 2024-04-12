#pragma once

#include <type_traits>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/BlockBloomFilter.h>
#include <parallel_hashmap/phmap_utils.h>
#include <Common/FieldVisitorHash.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

namespace DB
{

// #define FOR_NUMERIC_TYPES(M) \
// M(UInt8) \
// M(UInt16) \
// M(UInt32) \
// M(UInt64) \
// M(UInt128) \
// M(UInt256) \
// M(Int8) \
// M(Int16) \
// M(Int32) \
// M(Int64) \
// M(Int128) \
// M(Int256)

static size_t getFieldDiff(const Field & min, const Field & max)
{
    if (min.getType() == Field::Types::Int64)
        return max.safeGet<Int64>() - min.safeGet<Int64>() + 1;
    else if (min.getType() == Field::Types::UInt64)
        return max.safeGet<UInt64>() - min.safeGet<UInt64>() + 1;
    else if (min.getType() == Field::Types::Int128)
        return max.safeGet<Int128>() - min.safeGet<Int128>() + 1;
    else if (min.getType() == Field::Types::UInt128)
        return max.safeGet<UInt128>() - min.safeGet<UInt128>() + 1;
    else if (min.getType() == Field::Types::Int256)
        return max.safeGet<Int256>() - min.safeGet<Int256>() + 1;
    else if (min.getType() == Field::Types::UInt256)
        return max.safeGet<UInt256>() - min.safeGet<UInt256>() + 1;

    return 0;
}

template<typename T>
inline bool isDateValueRange(T v) {
    return 19700101 <= v && v <= 21000101;
}

static bool getFieldRangeDate(const Field & min, const Field & max)
{
    if (min.getType() == Field::Types::Int64)
        return isDateValueRange(max.safeGet<Int64>()) && isDateValueRange(min.safeGet<Int64>());
    else if (min.getType() == Field::Types::UInt64)
        return isDateValueRange(max.safeGet<UInt64>()) && isDateValueRange(min.safeGet<UInt64>());
    else if (min.getType() == Field::Types::Int128)
        return isDateValueRange(max.safeGet<Int128>()) && isDateValueRange(min.safeGet<Int128>());
    else if (min.getType() == Field::Types::UInt128)
        return isDateValueRange(max.safeGet<UInt128>()) && isDateValueRange(min.safeGet<UInt128>());
    else if (min.getType() == Field::Types::Int256)
        return isDateValueRange(max.safeGet<Int256>()) && isDateValueRange(min.safeGet<Int256>());
    else if (min.getType() == Field::Types::UInt256)
        return isDateValueRange(max.safeGet<UInt256>()) && isDateValueRange(min.safeGet<UInt256>());
    return false;
}

static bool isRangeAlmostMatchSet(const Field & min, const Field & max, size_t set_size) 
{
    return getFieldRangeDate(min, max) || (getFieldDiff(min, max) <= 1.5 * set_size);
}

class ValueSetWithRange
{
public:
    ValueSetWithRange() = default;

    explicit ValueSetWithRange(const DataTypePtr & dataType)
    {
        auto && min_max = dataType->getRange();
        if (min_max)
        {
            has_min_max = true;
            min = min_max->max;
            max = min_max->min;
        }
    }

    void insert(const Field & field)
    {
        if (has_min_max)
        {
            max = std::max(field, max);
            min = std::min(field, min);
        }
        set.emplace(field);
    }

    void merge(ValueSetWithRange & vs)
    {
        set.merge(vs.set);
        if (has_min_max && vs.has_min_max)
        {
            min = std::min(vs.min, min);
            max = std::max(vs.max, max);
        }
    }

    bool isRangeMatch() const
    {
        if (has_min_max) {
            if (getFieldDiff(min, max) == set.size())
                return true;
        }

        return false;
    }

    bool isRangeAlmostMatch() const
    {
        if (has_min_max) {
            return isRangeAlmostMatchSet(min, max, set.size());
        }

        return false;
    }

    void deserialize(ReadBuffer & buf);
    void serializeToBuffer(WriteBuffer & buf);

public:
    std::set<Field> set;
    Field min;
    Field max; // now only support numeric
    bool has_min_max = false;
};

struct MinMax {
    virtual ~MinMax() = default;
    virtual String dumpMin() const = 0;
    virtual String dumpMax() const = 0;
    virtual size_t getRangeSize() const = 0;
    virtual Field getMin() const = 0;
    virtual Field getMax() const = 0;
    virtual void addFieldKey(const Field & f) = 0;
};

template<typename  T>
struct MinMaxWrapper : public MinMax
{
    ~MinMaxWrapper() override = default;
    MinMaxWrapper() = default;
    explicit MinMaxWrapper(T min_, T max_) : min(min_), max(max_) {}

    T min{};
    T max{};

    void addFieldKey(const Field& f) override
    {
        min = std::min(min, f.safeGet<T>());
        max = std::max(max, f.safeGet<T>());
    }

    Field getMin() const override
    {
        return Field(min);
    }

    Field getMax() const override
    {
        return Field(max);
    }

    size_t getRangeSize() const override
    {
        return min - max + 1;
    }

    void addKey(T k)
    {
        min = std::min(k, min);
        max = std::max(k, max);
    }

    bool probInRange(T k) const
    {
        return k >= min && k <= max;
    }

    String dumpMin() const override
    {
        return Field(min).dump();
    }
    String dumpMax() const override
    {
        return Field(max).dump();
    }
};

class BloomFilterWithRange
{
public:
    enum ArchCheckType { RuntimeCheck, UseAVX2,  DoNotUseAVX2};
    BloomFilterWithRange() {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2)) {
            is_arch_supported = true;
        }
#endif
    }

    explicit BloomFilterWithRange(size_t ht_size, const DataTypePtr & dataType)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2)) {
            is_arch_supported = true;
        }
#endif
        bf.init(ht_size);
        auto && op_min_max = dataType->getRange();
        if (op_min_max)
        {
            has_min_max = true;
            initMinMax(op_min_max->max, op_min_max->min);
        }
    }

    void initMinMax(const Field & min, const Field & max)
    {
        switch (min.getType())
        {
            case Field::Types::UInt64:
            {
                min_max = std::make_unique<MinMaxWrapper<UInt64>>(min.get<UInt64>(), max.get<UInt64>());
                break ;
            }
            case Field::Types::Int64:
            {
                min_max = std::make_unique<MinMaxWrapper<Int64>>(min.get<Int64>(), max.get<Int64>());
                break ;
            }
            case Field::Types::Int128:
            {
                min_max = std::make_unique<MinMaxWrapper<Int128>>(min.get<Int128>(), max.get<Int128>());
                break ;
            }
            case Field::Types::UInt128:
            {
                min_max = std::make_unique<MinMaxWrapper<UInt128>>(min.get<UInt128>(), max.get<UInt128>());
                break ;
            }
            case Field::Types::UInt256:
            {
                min_max = std::make_unique<MinMaxWrapper<UInt256>>(min.get<UInt256>(), max.get<UInt256>());
                break ;
            }
            case Field::Types::Int256:
            {
                min_max = std::make_unique<MinMaxWrapper<Int256>>(min.get<Int256>(), max.get<Int256>());
                break ;
            }
            default:
            {
                has_min_max = false;
            }
        }
    }

    void addNull() { has_null = true;}

    template <typename KeyType>
    void addKey(const KeyType & key)
    {
        if (has_min_max)
        {
            if constexpr (std::is_same_v<Int128, KeyType>)
            {
                auto * k = static_cast<MinMaxWrapper<Int128> *>(min_max.get());
                k->addKey(key);
            }
            else if constexpr (std::is_same_v<UInt128, KeyType>)
            {
                auto * k = static_cast<MinMaxWrapper<UInt128> *>(min_max.get());
                k->addKey(key);
            }
            else if constexpr (std::is_same_v<UInt256, KeyType>)
            {
                auto * k = static_cast<MinMaxWrapper<UInt256> *>(min_max.get());
                k->addKey(key);
            }
            else if constexpr (std::is_same_v<Int256, KeyType>)
            {
                auto * k = static_cast<MinMaxWrapper<Int256> *>(min_max.get());
                k->addKey(key);
            }
            else if constexpr (std::is_signed_v<KeyType>)
            {
                auto * k = static_cast<MinMaxWrapper<Int64> *>(min_max.get());
                k->addKey(static_cast<Int64>(key));
            }
            else if constexpr (std::is_unsigned_v<KeyType>)
            {
                auto * k = static_cast<MinMaxWrapper<UInt64> *>(min_max.get());
                k->addKey(static_cast<UInt64>(key));
            }
        }
        size_t h;
        if constexpr(std::is_arithmetic_v<KeyType>  && (sizeof(KeyType) <= 8)) {
            h = phmap::phmap_mix<sizeof(size_t)>()(std::hash<KeyType>()(key));
            // bf.addKeyUnhash(h);
        } else {
            h = DefaultHash<KeyType>()(key);
            
        }
        bf.addKeyUnhash(h);
    }

    template <bool is_tuple>
    void addKey(const IColumn & column, size_t i)
    {
        if constexpr (is_tuple)
        {
            Field field;
            column.get(i, field);
            SipHash hash_state;
            applyVisitor(FieldVisitorHash(hash_state), field);
            bf.addKeyUnhash(hash_state.get64());
        }
        else
        {
            size_t h = DefaultHash<StringRef>()(column.getDataAt(i));
            bf.addKeyUnhash(h);
        }
    }

    template <typename KeyType, bool has_worker = false, ArchCheckType arch_check_type = ArchCheckType::RuntimeCheck, bool range_precheck = false>
    bool probeKey(const KeyType & key, UInt32 worker = 0) const
    {
        if constexpr (range_precheck) 
        {
            if constexpr (std::is_same_v<Int128, KeyType>)
            {
                const auto * k = static_cast<const MinMaxWrapper<Int128> *>(min_max.get());
                if (!k->probInRange(key))
                    return false;
            }
            else if constexpr (std::is_same_v<UInt128, KeyType>)
            {
                const auto * k = static_cast<const MinMaxWrapper<UInt128> *>(min_max.get());
                if (!k->probInRange(key))
                    return false;
            }
            else if constexpr (std::is_same_v<UInt256, KeyType>)
            {
                const auto * k = static_cast<const MinMaxWrapper<UInt256> *>(min_max.get());
                if (!k->probInRange(key))
                    return false;
            }
            else if constexpr (std::is_same_v<Int256, KeyType>)
            {
                const auto * k = static_cast<const MinMaxWrapper<Int256> *>(min_max.get());
                if (!k->probInRange(key))
                    return false;
            }
            else if constexpr (std::is_signed_v<KeyType>)
            {
                const auto * k = static_cast<const MinMaxWrapper<Int64> *>(min_max.get());
                if (!k->probInRange(key))
                    return false;
            }
            else if constexpr (std::is_unsigned_v<KeyType>)
            {
                const auto * k = static_cast<const MinMaxWrapper<UInt64> *>(min_max.get());
                if (!k->probInRange(key))
                    return false;
            }
        }
       
        size_t h;
        if constexpr(std::is_arithmetic_v<KeyType> && (sizeof(KeyType) <= 8)) {
            h = phmap::phmap_mix<sizeof(size_t)>()(std::hash<KeyType>()(key));
            // bf.addKeyUnhash(h);
        } else {
            h = DefaultHash<KeyType>()(key);
        }
        // size_t 
#if defined(__aarch64__) && defined(__ARM_NEON)
        if constexpr (has_worker)
            return part_bfs[worker].probeKeyUnhash(h);
        else
            return bf.probeKeyUnhash(h);
#else
        if constexpr (arch_check_type == ArchCheckType::RuntimeCheck) {
            if constexpr (has_worker)
                return part_bfs[worker].probeKeyUnhash(h);
            else
                return bf.probeKeyUnhash(h);
        } else if constexpr (arch_check_type == ArchCheckType::UseAVX2) {
            if constexpr (has_worker)
                return part_bfs[worker].probeKeyUnhashAvx2(h);
            else
                return bf.probeKeyUnhashAvx2(h);
        } else if constexpr (arch_check_type == ArchCheckType::DoNotUseAVX2) {
            if constexpr (has_worker)
                return part_bfs[worker].probeKeyUnhashScalar(h);
            else
                return bf.probeKeyUnhashScalar(h);
        } else {
            throw Exception("Unknown ArchCheckType", ErrorCodes::LOGICAL_ERROR);
        }
#endif
        
    }

    template <bool is_tuple, bool has_worker>
    bool probeKey(const IColumn & column, size_t i, UInt32 worker = 0) const
    {
        size_t h;
        if constexpr (is_tuple)
        {
            Field field;
            column.get(i, field);
            SipHash hash_state;
            applyVisitor(FieldVisitorHash(hash_state), field);
            h = hash_state.get64();
        }
        else
        {
            h = DefaultHash<StringRef>()(column.getDataAt(i));
        }

        if constexpr (has_worker)
            return part_bfs[worker].probeKeyUnhash(h);
        else
            return bf.probeKeyUnhash(h);
    }

    ArchCheckType getBestArchCheckType() const {
        if (is_arch_supported) {
            return ArchCheckType::UseAVX2;
        } else {
            return ArchCheckType::DoNotUseAVX2;
        }
    }

    void mergeInplace(BloomFilterWithRange && bfRange)
    {
        bf.mergeInplace(std::move(bfRange.bf));

        has_null |= bfRange.has_null;

        if (bfRange.has_min_max && has_min_max)
        {
            min_max->addFieldKey(bfRange.min_max->getMin());
            min_max->addFieldKey(bfRange.min_max->getMax());
        }
    }

    void initForConcat()
    {
        part_bfs.emplace_back(std::move(bf));
        num_partitions = part_bfs.size();
    }

    void concatInplace(BloomFilterWithRange && bfRange)
    {
        part_bfs.emplace_back(std::move(bfRange.bf));
        num_partitions = part_bfs.size();
        has_null |= bfRange.has_null;

        if (bfRange.has_min_max && has_min_max)
        {
            min_max->addFieldKey(bfRange.min_max->getMin());
            min_max->addFieldKey(bfRange.min_max->getMax());
        }
    }

    bool isRangeMatch() const
    {
        if (has_min_max) {
            if (min_max->getRangeSize() == bf.ndv)
                return true;
        }
        return false;
    }

    Field min() const {return min_max->getMin();}
    Field max() const {return min_max->getMax();}

    void deserialize(ReadBuffer & istr);
    void serializeToBuffer(WriteBuffer & ostr);

    void switchToLocal()
    {
        if (num_partitions != 1)
            return;
        bf = std::move(part_bfs[0]);
        num_partitions = 0;
    }
    void mergeValueSet(const ValueSetWithRange & valueSetWithRange);
    void addFieldKey(const Field & f); /// only use for merge value set
    String debugString() const;

public:
    BlockBloomFilter bf;
    std::unique_ptr<MinMax> min_max = nullptr;
    bool has_min_max = false;
    bool has_null = false;
    size_t num_partitions = 0;
    bool is_pre_enlarged = false;
    std::vector<BlockBloomFilter> part_bfs;
    bool is_arch_supported = false;
};


} // DB

