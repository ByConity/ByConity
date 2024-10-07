#pragma once

#include <Common/Logger.h>
#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/SignatureUsage.h>
#include <Analyzers/ASTEquals.h>
#include <Core/Names.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/Void.h>
#include <Poco/Logger.h>

#include <memory>
#include <optional>
#include <unordered_set>

namespace DB
{
class MaterializedViewAdvisor : public IWorkloadAdvisor
{
public:
    enum OutputType
    {
        PROJECTION,
        MATERIALIZED_VIEW
    };

    explicit MaterializedViewAdvisor(
        OutputType output_type_ = OutputType::MATERIALIZED_VIEW, bool only_aggregate_ = true, bool ignore_filter_ = true)
        : output_type(output_type_), only_aggregate(only_aggregate_), ignore_filter(ignore_filter_)
    {
    }

    String getName() const override { return "MaterializedViewAdvisor"; }

    WorkloadAdvises analyze(AdvisorContext & context) const override;

private:
        // compute the benefit of materializing a node, which is original_cost - SCALE_UP * scan_output_cost
    static std::optional<double> calculateMaterializeBenefit(const PlanNodePtr & node, const WorkloadQueryPtr & query, ContextPtr context);

    static void
    addChildrenToBlacklist(PlanNodePtr node, const PlanNodeToSignatures & signatures, std::unordered_set<PlanSignature> & blacklist);

    static std::vector<String> getRelatedQueries(const std::vector<String> & related_query_ids, AdvisorContext & context);

    const OutputType output_type;
    const bool only_aggregate;
    const bool ignore_filter;
    LoggerPtr log = getLogger("MaterializedViewAdvisor");
};

/**
 * @class MaterializedViewCandidate a candidate that can be converted to MV sql
 *
 */
struct MaterializedViewCandidate
{
public:
    PlanSignature plan_signature;
    QualifiedTableName table_name;
    double total_cost;
    bool contains_aggregate;
    EqualityASTSet wheres;
    EqualityASTSet group_bys;
    EqualityASTSet outputs;
    std::vector<String> related_queries;

    static std::optional<MaterializedViewCandidate> from(
        PlanNodePtr plan,
        const std::vector<String> & related_queries,
        PlanSignature plan_signature,
        double total_cost,
        bool only_aggregate,
        bool ignore_filter);
    bool tryMerge(const MaterializedViewCandidate & other);
    std::string toSql(MaterializedViewAdvisor::OutputType output_type) const;
    std::string toQuery() const;
    std::string toProjection() const;
};

class MaterializedViewAdvise : public IWorkloadAdvise
    {
    public:
        explicit MaterializedViewAdvise(QualifiedTableName table_, String sql_, std::vector<String> related_queries_, double benefit_)
            : table(std::move(table_)), sql(std::move(sql_)), benefit(benefit_), related_queries(std::move(related_queries_))
        {
    }

    String apply(WorkloadTables &) override
    {
        return "";
    }
    QualifiedTableName getTable() override
    {
        return table;
    }
    String getAdviseType() override
    {
        return "MaterializedView";
    }
    String getOriginalValue() override
    {
        return "";
    }
    String getOptimizedValue() override
    {
        return sql;
    }
    double getBenefit() override
    {
        return benefit;
    }
    std::vector<String> getRelatedQueries() override
    {
        return related_queries;
    }

    QualifiedTableName table;
    String sql;
    double benefit;
    std::vector<String> related_queries;
};

}
