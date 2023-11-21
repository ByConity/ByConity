#pragma once

#include <random>

#include <Storages/DiskCache/Buffer.h>
#include <common/types.h>

namespace DB::HybridCache
{
class BufferGen
{
public:
    explicit BufferGen(UInt32 seed = 1) : rg(seed) { }

    Buffer gen(UInt32 size)
    {
        Buffer buf{size};
        auto * p = buf.data();
        for (UInt32 i = 0; i < size; i++)
        {
            p[i] = static_cast<UInt8>(kAlphabet[rg() % (sizeof(kAlphabet) - 1)]);
        }
        return buf;
    }

    Buffer gen(UInt32 size_min, UInt32 size_max)
    {
        if (size_min == size_max)
            return gen(size_min);
        else
            return gen(size_min + static_cast<UInt32>(rg() % (size_max - size_min)));
    }

private:
    static constexpr char kAlphabet[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                          "abcdefghijklmnopqrstuvwxyz"
                                          "0123456789=+";
    std::minstd_rand rg;
};

template <typename T>
std::pair<double, double> getMeanDeviation(std::vector<T> v)
{
    double sum = std::accumulate(v.begin(), v.end(), 0.0);
    double mean = sum / v.size();

    double accum = 0.0;
    std::for_each(v.begin(), v.end(), [&](const T & d) { accum += (static_cast<double>(d) - mean) * (static_cast<double>(d) - mean); });

    return std::make_pair(mean, sqrt(accum / v.size()));
}
}
