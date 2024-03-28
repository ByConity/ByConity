#pragma once

#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/AdvisorContext.h>
#include <Advisor/SignatureUsage.h>
#include <Analyzers/ASTEquals.h>
#include <Core/Names.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
#include <QueryPlan/Void.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>

#include <memory>
#include <optional>
#include <unordered_set>

namespace DB
{

class MaterializedViewCandidate;
using MaterializedViewCandidateWithBenefit = std::pair<MaterializedViewCandidate, double>;

class MaterializedViewAdvisor : public IWorkloadAdvisor
{
public:
    explicit MaterializedViewAdvisor(bool _agg_only): agg_only(_agg_only) {}

    String getName() const override { return "MaterializedViewAdvisor"; }

    WorkloadAdvises analyze(AdvisorContext & context) const override;

protected:
    // MV size is very important because it also consumes storage, so we give more importance to the MV size
    float MV_SCAN_COST_SCALE_UP = 3.0;
    // max size of mv to be recommended
    size_t ADVISE_MAX_MV_SIZE = 100000;
    // min size of table for its mv to be recommended
    size_t ADVISE_MIN_TABLE_SIZE = 100000;

private:
    const bool agg_only;
    Poco::Logger * log = &Poco::Logger::get("MaterializedViewAdvisor");
    std::optional<MaterializedViewCandidateWithBenefit> buildCandidate(const SignatureUsageInfo & usage_info, ContextPtr context) const;
    // compute the benefit of materializing a node, which is original_cost - SCALE_UP * scan_output_cost
    std::optional<double> getMaterializeBenefit(std::shared_ptr<const PlanNodeBase> node,
                                                std::optional<double> original_cost,
                                                ContextPtr context) const;
};

/**
 * @class MaterializedViewCandidate a candidate that can be converted to MV sql
 *
 */
class MaterializedViewCandidate
{
public:
    class EqASTBySerialize
    {
    public:
        explicit EqASTBySerialize(ASTPtr _ast);
        ASTPtr getAST() const { return ast; }
        bool operator<(const EqASTBySerialize & other) const { return hash < other.hash; }
        bool operator==(const EqASTBySerialize & other) const { return hash == other.hash; }
    private:
        ASTPtr ast;
        UInt64 hash;
    };

    using EqASTBySerializeSet = std::set<EqASTBySerialize>;

    explicit MaterializedViewCandidate(
        QualifiedTableName table_name_,
        bool contains_aggregate_,
        EqASTBySerializeSet wheres_,
        EqASTBySerializeSet group_bys_,
        EqASTBySerializeSet outputs_)
        : table_name(std::move(table_name_))
        , contains_aggregate(contains_aggregate_)
        , wheres(std::move(wheres_))
        , group_bys(std::move(group_bys_))
        , outputs(std::move(outputs_))
    {
    }

    static std::optional<MaterializedViewCandidate> from(PlanNodePtr plan);

    bool tryMerge(const MaterializedViewCandidate & other);
    std::string toSQL();

    QualifiedTableName getTableName() const { return table_name; }
    bool containsAggregate() const { return contains_aggregate; }


private: // for test
    QualifiedTableName table_name;
    bool contains_aggregate;
    EqASTBySerializeSet wheres;
    EqASTBySerializeSet group_bys;
    EqASTBySerializeSet outputs;
};

}
