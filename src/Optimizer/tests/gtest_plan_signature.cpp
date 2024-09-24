#include <Optimizer/Signature/PlanSignature.h>

#include <Optimizer/PlanNodeSearcher.h>

#include <Optimizer/tests/gtest_base_tpcds_plan_test.h>
#include <gtest/gtest.h>
#include "common/logger_useful.h"

#include <string>
#include <memory>

using namespace DB;

class PlanSignatureTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, DB::Field> settings = BasePlanTest::getDefaultOptimizerSettings();
        tester = std::make_shared<BaseTpcdsPlanTest>(settings, 1000);
    }

    static void TearDownTestSuite() { tester.reset(); }

    PlanNodeToSignatures computeSignature(size_t query_id, const std::unordered_map<std::string, Field> & settings = {});

    static std::shared_ptr<BaseTpcdsPlanTest> tester;
};

std::shared_ptr<BaseTpcdsPlanTest> PlanSignatureTest::tester;

namespace
{
struct PlanNodeWithQueryId
{
    PlanNodePtr node;
    size_t query_id;
};

using PlanNodeWithQueryIds = std::vector<PlanNodeWithQueryId>;
using SignatureSummary = std::unordered_map<PlanSignature, PlanNodeWithQueryIds>;

void collectGroupBySignature(SignatureSummary & res, const PlanNodeToSignatures & sigs, size_t query_id)
{
    for (const auto & [node, sig] : sigs)
    {
        if (res.contains(sig))
            res[sig].emplace_back(PlanNodeWithQueryId{node, query_id});
        else
            res[sig] = PlanNodeWithQueryIds{PlanNodeWithQueryId{node, query_id}};
    }
}

void removeSingletons(SignatureSummary & res)
{
    for (auto it = res.begin(); it != res.end();)
    {
        if (it->second.size() <= 1)
            it = res.erase(it);
        else
            ++it;
    }
}

void simplifyChildren(SignatureSummary & res, const PlanNodeToSignatures & sigs)
{
    // we only check parent signatures, because same parent signature should always imply same children
    std::unordered_set<PlanSignature> children_to_remove;
    for (const auto & [node, sig] : sigs)
    {
        if (res.contains(sig))
        {
            for (const auto & child : node->getChildren()) {
                if (sigs.contains(child))
                    children_to_remove.insert(sigs.at(child));
            }
        }
    }
    for (const auto & sig : children_to_remove)
    {
        if (res.contains(sig))
            res.erase(sig);
    }
}

}

PlanNodeToSignatures PlanSignatureTest::computeSignature(size_t query_id,
                                                         const std::unordered_map<std::string, Field> & settings)
{
    auto query = tester->loadQuery("q" + std::to_string(query_id));
    auto sql = query.sql.front().first;
    auto query_settings = query.settings;
    for (const auto & setting: settings)
        query_settings.emplace(setting);
    auto context = tester->createQueryContext(query_settings);
    auto plan = tester->plan(sql, context);
    auto provider = PlanSignatureProvider::from(*plan, context);
    return provider.computeSignatures(plan->getPlanNode());
}

TEST_F(PlanSignatureTest, DISABLED_testQ1WithRuntimeFilter)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("enable_runtime_filter", "true");
    SignatureSummary res;
    PlanNodeToSignatures sigs = computeSignature(1, settings);
    collectGroupBySignature(res, sigs, 1);
    removeSingletons(res);
    // there are 2 patterns
    // Exchange-Aggregating-Projection-Filter-TableScan(store), the Filter-TableScan(store) appears 3 times
    // Exchange-Project-Filter-TableScan(date_dim)
    // so distinct repeated signatures = 8, total=8*2+2=18
    // with runtime filters, the signature of TableScan(store_returns) are not equal
    EXPECT_EQ(res.size(), 6);
}

TEST_F(PlanSignatureTest, DISABLED_testQ1WithoutRuntimeFilter)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("enable_runtime_filter", "false");
    SignatureSummary res;
    PlanNodeToSignatures sigs = computeSignature(1, settings);
    collectGroupBySignature(res, sigs, 1);
    removeSingletons(res);
    simplifyChildren(res, sigs);
    // there is one large pattern
    // select sr_customer_sk, sr_store_sk, sr_return_amt from (
    //      select sr_returned_date_sk, sr_customer_sk, sr_store_sk, sr_return_amt from store_returns
    //          where sr_store_sk in (select distinct s_store_sk from store)
    // ), date_dim where d_year = 2000 and sr_return_date_sk = d_date_sk
    EXPECT_EQ(res.size(), 1);
    auto repeats = (*res.begin()).second;
    EXPECT_EQ(repeats.size(), 2);
    // auto table_scan_nodes = PlanNodeSearcher::searchFrom(const_pointer_cast<PlanNodeBase>(repeats.front().node))
    //                             .where([](PlanNodeBase & node){ return node.getStep()->getType() == IQueryPlanStep::Type::TableScan;})
    //                             .findAll();
    // EXPECT_EQ(table_scan_nodes.size(), 1);
}

TEST_F(PlanSignatureTest, testTpcdsAllSignaturesWithRuntimeFilter)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("enable_runtime_filter", "true");
    SignatureSummary res;
    for (size_t i = 1; i <= 99; ++i)
    {
        PlanNodeToSignatures sigs = computeSignature(i, settings);
        collectGroupBySignature(res, sigs, i);
    }
    removeSingletons(res);
    std::vector<PlanNodeWithQueryIds> sorted_by_freq{};
    for (auto & [sig, nodes] : res)
    {
        if (nodes.empty())
            continue;
        auto type = nodes.front().node->getStep()->getType();
        if (type == IQueryPlanStep::Type::TableScan
            || type == IQueryPlanStep::Type::ReadNothing
            || type == IQueryPlanStep::Type::Exchange)
            continue;
        sorted_by_freq.emplace_back(std::move(nodes));
    }
    std::sort(sorted_by_freq.begin(), sorted_by_freq.end(),
              [](const auto & left, const auto & right){ return left.size() > right.size(); });
    // ASSERT_GE(sorted_by_freq.size(), 6);
    // select d_date_sk, d_year from date_dim where d_year = 2002
    // Q4 * 3, Q11 * 2, Q27 * 1, Q30 * 2, Q74 * 2, Q75 * 3
    // EXPECT_EQ(sorted_by_freq[0].size(), 13);
    // select d_date_sk, d_year from date_dim where d_year = 2001
    // Q4 * 3, Q11 * 2, Q13 * 1, Q36 * 1, Q74 * 2, Q75 * 3
    // EXPECT_EQ(sorted_by_freq[1].size(), 12);
    // select ca_address_sk, ca_gmt_offset from customer_address where ca_gmt_offset = -5
    // Q33 * 3, Q56 * 3, Q60 * 3, Q61 * 2
    // select d_date_sk, d_year from date_dim where d_year = 2000
    // Q1 * 2, Q7 * 1, Q26 * 1, Q48 * 1, Q78 * 3, Q81 * 2, Q85 * 1
    // EXPECT_EQ(sorted_by_freq[2].size(), 11);
    // EXPECT_EQ(sorted_by_freq[3].size(), 11);
    // select s_store_sk, s_name from store where s_store_name = 'ese'
    // Q88 * 8, Q96 * 1
    // select d_date_sk, d_date from date_dim where d_date between 11192 and 11222
    // Q77 * 6, Q80 * 2
    // EXPECT_EQ(sorted_by_freq[4].size(), 9);
    // EXPECT_EQ(sorted_by_freq[5].size(), 9);
}

TEST_F(PlanSignatureTest, testTpcdsAllSignaturesWithoutRuntimeFilter)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("enable_runtime_filter", "false");
    SignatureSummary res;
    for (size_t i = 1; i <= 99; ++i)
    {
        PlanNodeToSignatures sigs = computeSignature(i, settings);
        collectGroupBySignature(res, sigs, i);
    }
    removeSingletons(res);
    std::vector<PlanNodeWithQueryIds> sorted_by_freq{};
    for (auto & [sig, nodes] : res)
    {
        if (nodes.empty())
            continue;
        auto type = nodes.front().node->getStep()->getType();
        if (type != IQueryPlanStep::Type::Join && type != IQueryPlanStep::Type::Aggregating)
            continue;
        std::unordered_set<size_t> query_ids{};
        for (const auto & node : nodes)
            query_ids.insert(node.query_id);
        if (query_ids.size() == 1)
            continue;
        // explicitly rule out the Agg-ReadNothing pattern
        if (type == IQueryPlanStep::Type::Aggregating
            && nodes.front().node->getChildren().front()->getStep()->getType() == IQueryPlanStep::Type::ReadNothing)
            continue;
        sorted_by_freq.emplace_back(std::move(nodes));
    }
    std::sort(
        sorted_by_freq.begin(), sorted_by_freq.end(), [](const auto & left, const auto & right) { return left.size() > right.size(); });
    EXPECT_EQ(sorted_by_freq.size(), 12);
    // all binary mappings
    EXPECT_EQ(sorted_by_freq[0].size(), 2);
    // std::unordered_map<size_t, size_t> query_mapping;
    // for (const auto & nodes : sorted_by_freq)
    // {
    //     query_mapping.emplace(nodes[0].query_id, nodes[1].query_id);
    //     query_mapping.emplace(nodes[1].query_id, nodes[0].query_id);
    // }
    // EXPECT_EQ(query_mapping.size(), 6);
}
