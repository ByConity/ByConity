#pragma once

#include <random>

#include <Storages/DiskCache/Buffer.h>
#include <common/types.h>

namespace DB::HybridCache
{
class BufferGen
{
public:
    explicit BufferGen(UInt32 seed = 1) : rg_(seed) { }

    Buffer gen(UInt32 size)
    {
        Buffer buf{size};
        auto * p = buf.data();
        for (UInt32 i = 0; i < size; i++)
        {
            p[i] = static_cast<UInt8>(kAlphabet[rg_() % (sizeof(kAlphabet) - 1)]);
        }
        return buf;
    }

    Buffer gen(UInt32 size_min, UInt32 size_max)
    {
        if (size_min == size_max)
            return gen(size_min);
        else
            return gen(size_min + static_cast<UInt32>(rg_() % (size_max - size_min)));
    }

private:
    static constexpr char kAlphabet[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                          "abcdefghijklmnopqrstuvwxyz"
                                          "0123456789=+";
    std::minstd_rand rg_;
};
}
