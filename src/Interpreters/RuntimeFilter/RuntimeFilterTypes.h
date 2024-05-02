#pragma once

#include <Columns/ColumnVector.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/BlockBloomFilter.h>
#include <Common/FieldVisitorHash.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

namespace DB
{

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

    bool isRangeMatch() {
        if (has_min_max) {
            if (getFieldDiff(min, max) == set.size())
                return true;
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
    virtual void addKey(const Field & f) = 0;
};

template<typename  T>
struct MinMaxWrapper : public MinMax
{
    ~MinMaxWrapper() override = default;
    MinMaxWrapper() = default;
    explicit MinMaxWrapper(T min_, T max_) : min(min_), max(max_) {}

    T min{};
    T max{};

    void addKey(const Field& f) override
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
    BloomFilterWithRange() = default;

    explicit BloomFilterWithRange(size_t ht_size, const DataTypePtr & dataType)
    {
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
            min_max->addKey(Field(key));
        }

        size_t h = DefaultHash<KeyType>()(key);
        bf.addKeyUnhash(h);
    }

    void addKey(const StringRef & key)
    {
        size_t h = DefaultHash<StringRef>()(key);
        bf.addKeyUnhash(h);
    }

    template <typename KeyType, bool range_precheck = false>
    bool probeKey(const KeyType & key) const
    {
        if constexpr (range_precheck)
        {
            if constexpr (std::is_same_v<Int128, KeyType>)
            {
                if (has_min_max)
                {
                    const auto * k = static_cast<const MinMaxWrapper<Int128> *>(min_max.get());
                    if (!k->probInRange(key))
                        return false;
                }
            }
            else if constexpr (std::is_same_v<UInt128, KeyType>)
            {
                if (has_min_max)
                {
                    const auto * k = static_cast<const MinMaxWrapper<UInt128> *>(min_max.get());
                    if (!k->probInRange(key))
                        return false;
                }
            }
            else if constexpr (std::is_same_v<UInt256, KeyType>)
            {
                if (has_min_max)
                {
                    const auto * k = static_cast<const MinMaxWrapper<UInt256> *>(min_max.get());
                    if (!k->probInRange(key))
                        return false;
                }
            }
            else if constexpr (std::is_same_v<Int256, KeyType>)
            {
                if (has_min_max)
                {
                    const auto * k = static_cast<const MinMaxWrapper<Int256> *>(min_max.get());
                    if (!k->probInRange(key))
                        return false;
                }
            }
            else if constexpr (std::is_signed_v<KeyType>)
            {
                if (has_min_max)
                {
                    const auto * k = static_cast<const MinMaxWrapper<Int64> *>(min_max.get());
                    if (!k->probInRange(key))
                        return false;
                }
            }
            else if constexpr (std::is_unsigned_v<KeyType>)
            {
                if (has_min_max)
                {
                    const auto * k = static_cast<const MinMaxWrapper<UInt64> *>(min_max.get());
                    if (!k->probInRange(key))
                        return false;
                }
            }
        }

        size_t h = DefaultHash<KeyType>()(key);
        return bf.probeKeyUnhash(h);
    }

    bool probeKey(const StringRef & key)
    {
        size_t h = DefaultHash<StringRef>()(key);
        return bf.probeKeyUnhash(h);
    }

    void mergeInplace(BloomFilterWithRange && bfRange)
    {
        bf.mergeInplace(std::move(bfRange.bf));

        has_null |= bfRange.has_null;

        if (bfRange.has_min_max && has_min_max)
        {
            min_max->addKey(bfRange.min_max->getMin());
            min_max->addKey(bfRange.min_max->getMax());
        }
    }

    bool isRangeMatch() {
        if (has_min_max) {
            if (min_max->getRangeSize() == bf.ndv)
                return true;
        }
        return false;
    }

    Field Min() const {return min_max->getMin();}
    Field Max() const {return min_max->getMax();}

    void deserialize(ReadBuffer & istr);
    void serializeToBuffer(WriteBuffer & ostr);

    String debug_string()
    {
        if (has_min_max)
            return "ht size:" + std::to_string(bf.ndv) + " min:" + min_max->dumpMin() + " max:" + min_max->dumpMax() +
                " ndv:" + std::to_string(bf.ndv);
        else
            return "ht size:" + std::to_string(bf.ndv)  + " ndv:" + std::to_string(bf.ndv);
    }

public:
    BlockBloomFilter bf;
    std::unique_ptr<MinMax> min_max = nullptr;
    bool has_min_max = false;
    bool has_null = false;
};


} // DB

