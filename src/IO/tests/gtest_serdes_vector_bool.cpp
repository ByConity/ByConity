#include <gtest/gtest.h>

#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <ctime>
#include <vector>

using namespace DB;

TEST(SerDesVectorBoolTest, fix)
{
    for (int x = 0; x < 2; x++) {
        for (int i = 0; i < 130; i++) {
            auto wr = std::make_unique<MemoryWriteBuffer>(0);

            std::vector<bool> v;
            v.assign(i, x);
            writeBinary(v, *wr);

            auto rd = wr->tryGetReadBuffer();
            std::vector<bool> r;
            readBinary(r, *rd);

            ASSERT_EQ(v,  r) << "size " << i << " value " << x;
        }
    }
}

TEST(SerDesVectorBoolTest, random)
{
    std::srand(unsigned(std::time(nullptr)));

    for (int i = 0; i < 1024; i++) {
        auto wr = std::make_unique<MemoryWriteBuffer>(0);

        std::vector<bool> v;
        v.assign(i, false);
        std::generate(v.begin(), v.end(), std::rand);
        writeBinary(v, *wr);

        auto rd = wr->tryGetReadBuffer();
        std::vector<bool> r;
        readBinary(r, *rd);

        ASSERT_EQ(v, r);
    }
}
