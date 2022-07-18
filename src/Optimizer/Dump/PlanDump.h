#pragma once

#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Dump/Json2Pb.h>
#include <Parsers/IAST.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <Statistics/StatisticsBase.h>
#include <Poco/Zip/Compress.h>
#include <Poco/Zip/Decompress.h>
#include <Poco/Zip/ZipCommon.h>

namespace DB
{
using namespace Statistics;
StatisticsTag StatisticsTagFromString(const String & tag_string);
String StatisticsTagToString(StatisticsTag tag);
PVar tableJson(ContextPtr context, const String & db_name, const String & table_name);

void dumpQuery(const String & sql, ContextPtr context);
void dumpDdlStats(QueryPlan & plan, ContextMutablePtr context);
void dumpSetting(ContextPtr context);
void dumpClusterInfo(ContextPtr context, size_t parallel);
//void dumpExplain(QueryPlan & plan, ContextMutablePtr context);
void loadStats(ContextPtr context, const String & path);
void loadSettings(ContextMutablePtr context, const String & path);
//void loadOthers(ContextMutablePtr context, const String & path);
void zipDirectory(const String & des_file, const String & src_dir);
void unzipDirectory(const String & des_dir, const String & src_file);


class NodeDumper : public PlanNodeVisitor<Void, Void>
{
public:
    explicit NodeDumper(Poco::JSON::Object & out, Poco::JSON::Object & stats, ContextPtr contextt, CTEInfo * cte_info = nullptr)
        : query_ddl(out)
        , query_stats(stats)
        , context(contextt)
        , cte_helper(cte_info ? std::make_optional<CTEPreorderVisitHelper>(*cte_info) : std::nullopt)
    {
    }

    ~NodeDumper() override = default;

    Void visitPlanNode(PlanNodeBase &, Void &) override;
    Void visitTableScanNode(TableScanNode &, Void &) override;
    Void visitCTERefNode(CTERefNode & node, Void &) override;

private:
    Poco::JSON::Object & query_ddl;
    Poco::JSON::Object & query_stats;
    ContextPtr context;

    //need deal the syntax of with ...
    std::optional<CTEPreorderVisitHelper> cte_helper;
    //    bool with_id;
    std::unordered_set<String> visited_tables;
};
}
