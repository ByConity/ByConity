#pragma once
#include <Statistics/StatisticsBaseImpl.h>
#include <Common/Exception.h>

#include <Statistics/Base64.h>
#include <Statistics/DataSketchesHelper.h>

namespace DB::Statistics
{
class StatsCpcSketch : public StatisticsBase
{
public:
    static constexpr auto default_lg_k = 12;
    static constexpr auto tag = StatisticsTag::CpcSketch;
    StatsCpcSketch() : data(default_lg_k) { }

    String serialize() const override;
    void deserialize(std::string_view blob) override;
    StatisticsTag getTag() const override { return tag; }

    double get_estimate() const { return data.get_estimate(); }

    template <typename T>
    void update(const T & value)
    {
        if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, String>)
        {
            data.update(value);
        }
        else
        {
            static_assert(std::is_trivial_v<T> || std::is_same_v<UUID, T>);
            T v = value;
            data.update(&v, sizeof(v));
        }
    }

    void merge(const StatsCpcSketch & rhs)
    {
        datasketches::cpc_union un(default_lg_k);
        un.update(data);
        un.update(rhs.data);
        data = un.get_result();
    }

    // To human-readable text
    String to_string() const { return data.to_string(); }

private:
    datasketches::cpc_sketch data;
};

// transform ndv to integer, and make it no greater than count
inline UInt64 AdjustNdvWithCount(double ndv_estimate, UInt64 count)
{
    UInt64 int_ndv = std::llround(ndv_estimate);
    return std::min(int_ndv, count);
}

}
