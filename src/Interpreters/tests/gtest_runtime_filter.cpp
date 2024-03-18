/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionsHashing.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/BloomFilter.h>
#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterTypes.h>
#include <Common/LinkedHashMap.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <random>
#include <gtest/gtest.h>

using namespace DB;

constexpr size_t DEFAULT_BLOOM_FILTER_BYTES = 1024 * 256;
constexpr size_t DEFAULT_BLOOM_HASH_NUM = 4;
constexpr size_t DEFAULT_BLOOM_FILTER_BITS = DEFAULT_BLOOM_FILTER_BYTES << 3;

TEST(RuntimeFilterTest, BloomFilterSerDer)
{
    BloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES, DEFAULT_BLOOM_HASH_NUM, DEFAULT_BLOOM_FILTER_BITS};

    StringRef key1("a");
    bloom_filter.add(key1.data, key1.size);

    StringRef key2("b");
    bloom_filter.add(key2.data, key2.size);

    /**
     * serialize to buffer
     */
    WriteBufferFromOwnString write_buffer;
    bloom_filter.serializeToBuffer(write_buffer);

    /**
     * deserialize from buffer
     */
    ReadBufferFromString read_buffer(write_buffer.str());
    BloomFilter new_bloom_filter{DEFAULT_BLOOM_FILTER_BYTES, DEFAULT_BLOOM_HASH_NUM, DEFAULT_BLOOM_FILTER_BITS};
    new_bloom_filter.deserialize(read_buffer);

    EXPECT_TRUE(new_bloom_filter.find(key1.data, key1.size));
    EXPECT_TRUE(new_bloom_filter.find(key2.data, key2.size));

    StringRef key3("c");
    EXPECT_FALSE(new_bloom_filter.find(key3.data, key3.size));
}

TEST(RuntimeFilterTest, BloomFilterMerge)
{
    BloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES, DEFAULT_BLOOM_HASH_NUM, DEFAULT_BLOOM_FILTER_BITS};
    StringRef key1("a");
    bloom_filter.add(key1.data, key1.size);

    BloomFilter bloom_filter2{DEFAULT_BLOOM_FILTER_BYTES, DEFAULT_BLOOM_HASH_NUM, DEFAULT_BLOOM_FILTER_BITS};
    StringRef key2("b");
    bloom_filter2.add(key2.data, key2.size);

    EXPECT_FALSE(bloom_filter.find(key2.data, key2.size));
    bloom_filter.merge(bloom_filter2);
    EXPECT_TRUE(bloom_filter.find(key2.data, key2.size));
}

TEST(RuntimeFilterTest, BloomFilterMerge2)
{
    BloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES, DEFAULT_BLOOM_HASH_NUM, DEFAULT_BLOOM_FILTER_BITS};

    auto col = ColumnVector<UInt64>::create();
    col->insert(Field{794873});
    col->insert(Field{1190443});
    auto key1 = col->getDataAt(0);
    auto key2 = col->getDataAt(1);
    bloom_filter.add(key1.data, key1.size);

    BloomFilter bloom_filter2{DEFAULT_BLOOM_FILTER_BYTES, DEFAULT_BLOOM_HASH_NUM, DEFAULT_BLOOM_FILTER_BITS};
    bloom_filter2.add(key2.data, key2.size);

    EXPECT_FALSE(bloom_filter.find(key2.data, key2.size));
    bloom_filter.merge(bloom_filter2);
    EXPECT_TRUE(bloom_filter.find(key2.data, key2.size));
}

TEST(RuntimeFilterTest, BlockBloomFilter)
{
    BlockBloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES};

    auto col = ColumnVector<UInt64>::create();
    bloom_filter.addKey(794873);
    bloom_filter.addKey(1190443);
    bloom_filter.addKey(12121237);

    EXPECT_TRUE(bloom_filter.probeKey(794873));
    EXPECT_TRUE(bloom_filter.probeKey(1190443));
    EXPECT_TRUE(bloom_filter.probeKey(12121237));
}

TEST(RuntimeFilterTest, BlockBloomFilterRandomTest)
{
    BlockBloomFilter bloom_filter{DEFAULT_BLOOM_FILTER_BYTES};

    auto col = ColumnVector<UInt64>::create();
    std::vector<int> random_numbers;
    for (int i = 0; i < 10000; i++) {
        random_numbers.push_back(rand());
    }

    for (const auto num : random_numbers) {
        EXPECT_FALSE(bloom_filter.probeKey(num));
    }

    for (const auto num : random_numbers) {
        bloom_filter.addKey(num);
    }

    for (const auto num : random_numbers) {
        EXPECT_TRUE(bloom_filter.probeKey(num));
    }
}

TEST(RuntimeFilterTest, DiffMerge1)
{
    Settings setting;
    LinkedHashMap<String, RuntimeFilterBuildInfos> rfs;

    RuntimeFilterBuildInfos rf1{1, RuntimeFilterDistribution::LOCAL};
    RuntimeFilterBuildInfos rf2{2, RuntimeFilterDistribution::DISTRIBUTED};

    rfs.emplace_back("local1", rf1);
    rfs.emplace_back("distr1", rf2);

    std::map<UInt32, RuntimeFilterData>  data_sets;

    /// worker 1
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk1;
    DataTypePtr type_ptr = std::make_shared<DataTypeNumber<Int32>>();
    BloomFilterWithRangePtr bloom_wk1 = std::make_shared<BloomFilterWithRange>(10124, type_ptr);
    for (int i = 0; i < 10124; ++i)
    {
        bloom_wk1->addKey(i);
    }
    bloom_wk1->is_pre_enlarged = true;
    RuntimeFilterVal wk1{true, bloom_wk1, nullptr };
    runtime_wk1.emplace(1, wk1);
    data_sets.insert(std::make_pair(0, RuntimeFilterData{std::move(runtime_wk1)}));

    /// worker 2
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk2;
    ValueSetWithRangePtr value_wk2 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 1023; ++i)
    {
        value_wk2->insert(i);
    }

    RuntimeFilterVal wk2{false, nullptr, value_wk2 };
    runtime_wk2.emplace(1, wk2);
    data_sets.insert(std::make_pair(1, RuntimeFilterData{std::move(runtime_wk2)}));


   /// worker 3
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk3;
    BloomFilterWithRangePtr bloom_wk3 = std::make_shared<BloomFilterWithRange>(10124, type_ptr);
    for (int i = 0; i < 10124; ++i)
    {
        bloom_wk3->addKey(i);
    }
    bloom_wk3->is_pre_enlarged = false;
    RuntimeFilterVal wk3{true, bloom_wk3, nullptr };
    runtime_wk3.emplace(1, wk3);
    data_sets.insert(std::make_pair(2, RuntimeFilterData{std::move(runtime_wk3)}));



    /// start build
    RuntimeFilterBuilder builder(setting, rfs);
    auto && dt = builder.merge(std::move(data_sets));
    EXPECT_EQ(dt.bypass, BypassType::NO_BYPASS);
    EXPECT_EQ(dt.runtime_filters.size(), 0);

}



TEST(RuntimeFilterTest, DiffMerge2)
{
    Settings setting;
    LinkedHashMap<String, RuntimeFilterBuildInfos> rfs;

    RuntimeFilterBuildInfos rf1{1, RuntimeFilterDistribution::LOCAL};
    RuntimeFilterBuildInfos rf2{2, RuntimeFilterDistribution::DISTRIBUTED};

    rfs.emplace_back("local1", rf1);
    rfs.emplace_back("distr1", rf2);

    std::map<UInt32, RuntimeFilterData>  data_sets;

    /// worker 1
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk1;
    DataTypePtr type_ptr = std::make_shared<DataTypeNumber<Int32>>();
    ValueSetWithRangePtr value_wk1 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 10124; ++i)
    {
        value_wk1->insert(i);
    }
    RuntimeFilterVal wk1{false, nullptr, value_wk1 };
    runtime_wk1.emplace(1, wk1);
    data_sets.insert(std::make_pair(0, RuntimeFilterData{std::move(runtime_wk1)}));

    /// worker 2
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk2;
    ValueSetWithRangePtr value_wk2 = std::make_shared<ValueSetWithRange>(type_ptr);
    for (int i = 0; i < 1023; ++i)
    {
        value_wk2->insert(i);
    }

    RuntimeFilterVal wk2{false, nullptr, value_wk2 };
    runtime_wk2.emplace(1, wk2);
    data_sets.insert(std::make_pair(1, RuntimeFilterData{std::move(runtime_wk2)}));


   /// worker 3
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_wk3;
    BloomFilterWithRangePtr bloom_wk3 = std::make_shared<BloomFilterWithRange>(10124, type_ptr);
    for (int i = 0; i < 10124; ++i)
    {
        bloom_wk3->addKey(i);
    }
    bloom_wk3->is_pre_enlarged = true;
    RuntimeFilterVal wk3{true, bloom_wk3, nullptr };
    runtime_wk3.emplace(1, wk3);
    data_sets.insert(std::make_pair(2, RuntimeFilterData{std::move(runtime_wk3)}));


    /// start build
    RuntimeFilterBuilder builder(setting, rfs);
    auto && dt = builder.merge(std::move(data_sets));
    EXPECT_EQ(dt.bypass, BypassType::NO_BYPASS);
    EXPECT_EQ(dt.runtime_filters.size(), 1);
    EXPECT_EQ(dt.runtime_filters[1].is_bf, true);


}
