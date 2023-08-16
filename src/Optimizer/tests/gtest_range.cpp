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

#include <Optimizer/value_sets.h>
#include <iostream>
#include <DataTypes/DataTypeFactory.h>
#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Common/Stopwatch.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace DB::Predicate;
using PredicateRange = Predicate::Range;


class RangeTest : public testing::Test
{
public:
    auto typeFromString(const std::string & str)
    {
        auto & data_type_factory = DataTypeFactory::instance();
        return data_type_factory.get(str);
    }

protected:
    RangeTest()
    {
        type_int64 = typeFromString("Int64");
    }

    ~RangeTest() {}
    DataTypePtr type_int64;
    Field field_0 = Int64(0);
    Field field_1 = Int64(1);
    Field field_2 = Int64(2);
    Field field_3 = Int64(3);
    Field field_10 = Int64(10);

};

TEST_F(RangeTest, testInvertedBounds)
{
    //    EXPECT_ANY_THROW(PredicateRange(typeFromString("String"), true, String("ab"), true, String("a")));
    //
    //    EXPECT_ANY_THROW(PredicateRange(typeFromString("Int64"), true, Field(Int64(1)), true, Field(Int64(0))));
}

TEST_F(RangeTest, testSingleValueExclusive)
{
    //    // (10, 10]
    //    EXPECT_ANY_THROW(PredicateRange(typeFromString("Int64"), false, Field(Int64(10)), true, Field(Int64(10))));
    //
    //    // [10, 10)
    //    EXPECT_ANY_THROW(PredicateRange(typeFromString("Int64"), true, Field(Int64(10)), false, Field(Int64(10))));
    //
    //    // (10, 10)
    //    EXPECT_ANY_THROW(Predicate::PredicateRange(typeFromString("Int64"), false, Field(Int64(10)), false, Field(Int64(10))));
}

TEST_F(RangeTest, testSingleValue)
{
    ASSERT_TRUE(PredicateRange(typeFromString("Int64"), true, Field(Int64(1)), true, Field(Int64(1))).isSingleValue());
    ASSERT_FALSE(PredicateRange(typeFromString("Int64"), true, Field(Int64(1)), true, Field(Int64(2))).isSingleValue());
    ASSERT_TRUE(PredicateRange(typeFromString("Float64"), true, Field(Float64(1.1)), true, Field(Float64(1.1))).isSingleValue());
    ASSERT_FALSE(PredicateRange(typeFromString("String"), true, Field(String("a")), true, Field(String("ab"))).isSingleValue());
}

TEST_F(RangeTest, testAllRange)
{
    PredicateRange range = PredicateRange::allRange(typeFromString("Int64"));

    ASSERT_TRUE(range.isLowUnbounded());
    ASSERT_FALSE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue().isNull());

    ASSERT_TRUE(range.isHighUnbounded());
    ASSERT_FALSE(range.isHighInclusive());
    ASSERT_TRUE(range.getHighValue().isNull());

    ASSERT_FALSE(range.isSingleValue());
    ASSERT_TRUE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testGreaterThanRange)
{
    PredicateRange range = PredicateRange::greaterThanRange(typeFromString("Int64"), Field(Int64(1)));

    ASSERT_FALSE(range.isLowUnbounded());
    ASSERT_FALSE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue() == Field(Int64(1)));

    ASSERT_TRUE(range.isHighUnbounded());
    ASSERT_FALSE(range.isHighInclusive());
    ASSERT_TRUE(range.getHighValue().isNull());

    ASSERT_FALSE(range.isSingleValue());
    ASSERT_FALSE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testGreaterThanOrEqualRange)
{
    PredicateRange range = PredicateRange::greaterThanOrEqualRange(typeFromString("Int64"), Field(Int64(1)));

    ASSERT_FALSE(range.isLowUnbounded());
    ASSERT_TRUE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue() == Field(Int64(1)));

    ASSERT_TRUE(range.isHighUnbounded());
    ASSERT_FALSE(range.isHighInclusive());
    ASSERT_TRUE(range.getHighValue().isNull());

    ASSERT_FALSE(range.isSingleValue());
    ASSERT_FALSE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testLessThanRange)
{
    PredicateRange range = PredicateRange::lessThanRange(typeFromString("Int64"), Field(Int64(1)));

    ASSERT_TRUE(range.isLowUnbounded());
    ASSERT_FALSE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue().isNull());

    ASSERT_FALSE(range.isHighUnbounded());
    ASSERT_FALSE(range.isHighInclusive());
    ASSERT_TRUE(range.getHighValue() == Field(Int64(1)));

    ASSERT_FALSE(range.isSingleValue());
    ASSERT_FALSE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testLessThanOrEqualRange)
{
    PredicateRange range = PredicateRange::lessThanOrEqualRange(typeFromString("Int64"), Field(Int64(1)));

    ASSERT_TRUE(range.isLowUnbounded());
    ASSERT_FALSE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue().isNull());

    ASSERT_FALSE(range.isHighUnbounded());
    ASSERT_TRUE(range.isHighInclusive());
    ASSERT_TRUE(range.getHighValue() == Field(Int64(1)));

    ASSERT_FALSE(range.isSingleValue());
    ASSERT_FALSE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testEqualRange)
{
    PredicateRange range = PredicateRange::equalRange(typeFromString("Int64"), Field(Int64(1)));

    ASSERT_FALSE(range.isLowUnbounded());
    ASSERT_TRUE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue() == Field(Int64(1)));

    ASSERT_FALSE(range.isHighUnbounded());
    ASSERT_TRUE(range.isHighInclusive());
    ASSERT_TRUE(range.getLowValue() == Field(Int64(1)));

    ASSERT_TRUE(range.isSingleValue());
    ASSERT_FALSE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testRange)
{
    //(0,2]
    PredicateRange range = PredicateRange(typeFromString("Int64"), false, Field(Int64(0)), true, Field(Int64(2)));
    ASSERT_FALSE(range.isLowUnbounded());
    ASSERT_FALSE(range.isLowInclusive());
    ASSERT_TRUE(range.getLowValue() == Field(Int64(0)));

    ASSERT_FALSE(range.isHighUnbounded());
    ASSERT_TRUE(range.isHighInclusive());
    ASSERT_TRUE(range.getHighValue() == Field(Int64(2)));

    ASSERT_FALSE(range.isSingleValue());
    ASSERT_FALSE(range.isAll());
    ASSERT_TRUE(range.getType()->getTypeId() == typeFromString("Int64")->getTypeId());
}

TEST_F(RangeTest, testGetSingleValue)
{
    ASSERT_TRUE(PredicateRange::equalRange(typeFromString("Int64"), Field(Int64(0))).getSingleValue() == Field(Int64(0)));
    //    try
    //    {
    //        PredicateRange::lessThanRange(typeFromString("Int64"), Field(Int64(0))).getSingleValue();
    //    }
    //    catch (...)
    //    {
    //        std::cerr << "PredicateRange does not have just a single value" << std::endl;
    //    }
}

TEST_F(RangeTest, testContains)
{
    ASSERT_TRUE(PredicateRange::allRange(type_int64).contains(PredicateRange::allRange(type_int64)));
    ASSERT_TRUE(PredicateRange::allRange(type_int64).contains(PredicateRange::equalRange(type_int64, field_0)));
    ASSERT_TRUE(PredicateRange::allRange(type_int64).contains(PredicateRange::greaterThanRange(type_int64, field_0)));
    ASSERT_TRUE(PredicateRange::equalRange(type_int64, field_0).contains(PredicateRange::equalRange(type_int64, field_0)));
    ASSERT_FALSE(PredicateRange::equalRange(type_int64, field_0).contains(PredicateRange::greaterThanRange(type_int64, field_0)));
    ASSERT_FALSE(PredicateRange::equalRange(type_int64, field_0).contains(PredicateRange::greaterThanOrEqualRange(type_int64, field_0)));
    ASSERT_FALSE(PredicateRange::equalRange(type_int64, field_0).contains(PredicateRange::allRange(type_int64)));
    ASSERT_TRUE(
        PredicateRange::greaterThanOrEqualRange(type_int64, field_0).contains(PredicateRange::greaterThanRange(type_int64, field_0)));
    ASSERT_TRUE(PredicateRange::greaterThanRange(type_int64, field_0).contains(PredicateRange::greaterThanRange(type_int64, field_1)));
    ASSERT_FALSE(PredicateRange::greaterThanRange(type_int64, field_0).contains(PredicateRange::lessThanRange(type_int64, field_0)));
    ASSERT_TRUE(
        PredicateRange(type_int64, true, field_0, true, field_2).contains(PredicateRange(type_int64, true, field_1, true, field_2)));
    ASSERT_FALSE(
        PredicateRange(type_int64, true, field_0, true, field_2).contains(PredicateRange(type_int64, true, field_1, false, field_3)));
}

TEST_F(RangeTest, testSpan)
{
    ASSERT_TRUE(
        PredicateRange::greaterThanRange(type_int64, field_1).span(PredicateRange::lessThanOrEqualRange(type_int64, field_2))
        == PredicateRange::allRange(type_int64));
    ASSERT_TRUE(
        PredicateRange::greaterThanRange(type_int64, field_2).span(PredicateRange::lessThanOrEqualRange(type_int64, field_0))
        == PredicateRange::allRange(type_int64));
    ASSERT_TRUE(
        PredicateRange(type_int64, true, field_1, false, field_3).span(PredicateRange::equalRange(type_int64, field_2))
        == PredicateRange(type_int64, true, field_1, false, field_3));
    ASSERT_TRUE(
        PredicateRange(type_int64, true, field_1, false, field_3).span(PredicateRange(type_int64, false, field_2, false, field_10))
        == PredicateRange(type_int64, true, field_1, false, field_10));
    ASSERT_TRUE(
        PredicateRange::greaterThanRange(type_int64, field_1).span(PredicateRange::equalRange(type_int64, field_0))
        == PredicateRange::greaterThanOrEqualRange(type_int64, field_0));
    ASSERT_TRUE(
        PredicateRange::greaterThanRange(type_int64, field_1).span(PredicateRange::greaterThanOrEqualRange(type_int64, field_10))
        == PredicateRange::greaterThanRange(type_int64, field_1));
    ASSERT_TRUE(
        PredicateRange::lessThanRange(type_int64, field_1).span(PredicateRange::lessThanOrEqualRange(type_int64, field_1))
        == PredicateRange::lessThanOrEqualRange(type_int64, field_1));
    ASSERT_TRUE(
        PredicateRange::allRange(type_int64).span(PredicateRange::lessThanOrEqualRange(type_int64, field_1))
        == PredicateRange::allRange(type_int64));
}

TEST_F(RangeTest, testOverlaps)
{
    ASSERT_TRUE(PredicateRange::greaterThanRange(type_int64, field_1).overlaps(PredicateRange::lessThanOrEqualRange(type_int64, field_2)));
    ASSERT_FALSE(PredicateRange::greaterThanRange(type_int64, field_2).overlaps(PredicateRange::lessThanRange(type_int64, field_2)));
    ASSERT_TRUE(PredicateRange(type_int64, true, field_1, false, field_3).overlaps(PredicateRange::equalRange(type_int64, field_2)));
    ASSERT_TRUE(
        PredicateRange(type_int64, true, field_1, false, field_3).overlaps(PredicateRange(type_int64, false, field_2, false, field_10)));
    ASSERT_FALSE(
        PredicateRange(type_int64, true, field_1, false, field_3).overlaps(PredicateRange(type_int64, true, field_3, false, field_10)));
    ASSERT_TRUE(
        PredicateRange(type_int64, true, field_1, true, field_3).overlaps(PredicateRange(type_int64, true, field_3, false, field_10)));
    ASSERT_TRUE(PredicateRange::allRange(type_int64).overlaps(PredicateRange::equalRange(type_int64, type_int64->getRange().value().max)));
}

TEST_F(RangeTest, testIntersect)
{
    std::optional<PredicateRange> range_1
        = PredicateRange::greaterThanRange(type_int64, field_1).intersect(PredicateRange::lessThanOrEqualRange(type_int64, field_2));
    ASSERT_TRUE(range_1.has_value());
    ASSERT_TRUE(range_1.value() == PredicateRange(type_int64, false, field_1, true, field_2));

    std::optional<PredicateRange> range_2
        = PredicateRange(type_int64, true, field_1, false, field_3).intersect(PredicateRange::equalRange(type_int64, field_2));
    ASSERT_TRUE(range_2.has_value());
    ASSERT_TRUE(range_2 == PredicateRange::equalRange(type_int64, field_2));

    std::optional<PredicateRange> range_3
        = PredicateRange(type_int64, true, field_1, false, field_3).intersect(PredicateRange(type_int64, false, field_2, false, field_10));
    ASSERT_TRUE(range_3.has_value());
    ASSERT_TRUE(range_3 == PredicateRange(type_int64, false, field_2, false, field_3));

    std::optional<PredicateRange> range_4
        = PredicateRange(type_int64, true, field_1, true, field_3).intersect(PredicateRange(type_int64, true, field_3, false, field_10));
    ASSERT_TRUE(range_4.has_value());
    ASSERT_TRUE(range_4 == PredicateRange::equalRange(type_int64, field_3));

    std::optional<PredicateRange> range_5
        = PredicateRange::allRange(type_int64).intersect(PredicateRange::equalRange(type_int64, type_int64->getRange().value().max));
    ASSERT_TRUE(range_5.has_value());
    ASSERT_TRUE(range_5 == PredicateRange::equalRange(type_int64, type_int64->getRange().value().max));
}


TEST_F(RangeTest, testExceptionalIntersect)
{
    PredicateRange greater_than_2 = PredicateRange::greaterThanRange(type_int64, field_2);
    PredicateRange less_than_2 = PredicateRange::lessThanRange(type_int64, field_2);
    ASSERT_FALSE(greater_than_2.intersect(less_than_2).has_value());

    PredicateRange range_1_to_3_Exclusive = PredicateRange(type_int64, true, field_1, false, field_3);
    PredicateRange range_3_to_10 = PredicateRange(type_int64, true, field_3, false, field_10);
    ASSERT_FALSE(range_1_to_3_Exclusive.intersect(range_3_to_10).has_value());
}

//int main(int argc, char **argv)
//{
//    ::testing::InitGoogleTest(&argc, argv);
//    return RUN_ALL_TESTS();
//}
