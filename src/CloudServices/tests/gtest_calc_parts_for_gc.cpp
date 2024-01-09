#include <CloudServices/CnchPartsHelper.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>

#include <iostream>

using namespace DB;

static MinimumDataPartPtr P(const String & name) { return MinimumDataPart::create(name, false); }
static MinimumDataPartPtr D(const String & name) { return MinimumDataPart::create(name, true); }

static void checkResult(std::function<MinimumDataParts()> creator, const String & expected_to_gc, const String & expected_visible)
{
    {
        auto input = creator();
        MinimumDataParts to_gc;
        CnchPartsHelper::calcMinimumPartsForGC(input, &to_gc, nullptr);
        WriteBufferFromOwnString buf;
        for (size_t i = 0; i < to_gc.size(); ++i)
            buf << (i ? "," : "") << to_gc[i]->name;
        EXPECT_EQ(buf.str(), expected_to_gc);
    }

    {
        auto input = creator();
        MinimumDataParts visible;
        CnchPartsHelper::calcMinimumPartsForGC(input, nullptr, &visible);
        WriteBufferFromOwnString buf;
        for (size_t i = 0; i < visible.size(); ++i)
        {
            buf << (i ? "," : "") << visible[i]->name;
            for (auto p = visible[i]->prev; p; p = p->prev)
                buf << "<-" << p->name;
        }
        EXPECT_EQ(buf.str(), expected_visible);
    }
}

TEST(TestCalcPartsForGC, Basic)
{
    checkResult(
        []() -> MinimumDataParts { return {}; },
        "",
        ""
    );

    checkResult(
        []() -> MinimumDataParts {
            return { P("all_0_10_0_1000") };
        },
        "",
        "all_0_10_0_1000"
    );

    checkResult(
        []() -> MinimumDataParts {
            return {
                P("all_1_1_0_1000"), P("all_2_2_0_1001"), P("all_3_3_0_1002"), // insert 1,2,3
                D("all_1_1_1_1003"), D("all_3_3_1_1003"), P("all_1_3_1_1003"), // merge 1 and 3
                P("all_1_3_2_1004"), P("all_2_2_1_1004") // mutate
            };
        },
        "all_1_1_0_1000,all_3_3_0_1002",
        "all_1_1_1_1003<-all_1_1_0_1000,all_1_3_2_1004<-all_1_3_1_1003,all_2_2_1_1004<-all_2_2_0_1001,all_3_3_1_1003<-all_3_3_0_1002"
    );

    checkResult(
        []() -> MinimumDataParts {
            return {
                P("all_1_1_0_1000"), P("all_2_2_0_1001"), P("all_3_3_0_1002"), // insert 1,2,3
                D("all_1_1_1_1003"), D("all_3_3_1_1003"), P("all_1_3_1_1003"), // merge 1 and 3
                P("all_1_3_2_1004"), P("all_2_2_1_1004"), // mutate
                D("all_0_3_999999999_1005") // drop range
            };
        },
        "all_1_1_0_1000,all_1_3_2_1004,all_1_3_1_1003,all_2_2_1_1004,all_2_2_0_1001,all_3_3_0_1002",
        "all_1_1_1_1003<-all_1_1_0_1000,all_1_3_2_1004<-all_1_3_1_1003,all_2_2_1_1004<-all_2_2_0_1001,all_3_3_1_1003<-all_3_3_0_1002,all_0_3_999999999_1005"
    );

    checkResult(
        []() -> MinimumDataParts {
            return {
                D("all_1_1_1_1003"), D("all_3_3_1_1003"), D("all_0_3_999999999_1005")
            };
        },
        "all_1_1_1_1003,all_3_3_1_1003",
        "all_1_1_1_1003,all_3_3_1_1003,all_0_3_999999999_1005"
    );
}

TEST(TestCalcPartsForGC, CleanRangeTombstones)
{
    checkResult(
        []() -> MinimumDataParts {
            return { D("all_0_10_999999999_1000") };
        },
        "all_0_10_999999999_1000",
        "all_0_10_999999999_1000"
    );

    // parts covered by tombstone
    checkResult(
        []() -> MinimumDataParts {
            return { P("all_9_9_0_1000"), P("all_10_10_0_1001"), D("all_0_10_999999999_1002")  };
        },
        "all_9_9_0_1000,all_10_10_0_1001",
        "all_9_9_0_1000,all_10_10_0_1001,all_0_10_999999999_1002"
    );

    // different partitions
    checkResult(
        []() -> MinimumDataParts {
            return { P("0_10_10_0_1000"), D("1_0_10_999999999_1001")  };
        },
        "1_0_10_999999999_1001",
        "0_10_10_0_1000,1_0_10_999999999_1001"
    );

    // drop range parts with the same block name
    checkResult(
        []() -> MinimumDataParts {
            return {
                D("1_0_10_999999999_1000"), D("1_0_10_999999999_1001") };
        },
        "1_0_10_999999999_1000,1_0_10_999999999_1001",
        "1_0_10_999999999_1000,1_0_10_999999999_1001"
    );

    checkResult(
        []() -> MinimumDataParts {
            return { D("all_0_10_999999999_1000"), D("all_0_11_999999999_1001"), P("all_12_12_0_1002"), D("all_0_12_999999999_1003"), P("all_13_13_0_1004") };
        },
        "all_12_12_0_1002,all_0_10_999999999_1000,all_0_11_999999999_1001",
        "all_12_12_0_1002,all_13_13_0_1004,all_0_10_999999999_1000,all_0_11_999999999_1001,all_0_12_999999999_1003"
    );
}
