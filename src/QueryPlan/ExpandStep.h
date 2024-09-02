#pragma once

#include <vector>
#include <Core/NameToType.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/ITransformingStep.h>
#include <common/types.h>
namespace DB
{
/**
 * ExpandStep will expand data multiple times according group_id values.
 *
 * For simplicity, we assume that the source data are read by on two nodes and 
 * there are 8 rows:
 *
 *       Node 1                          Node 2
 *
 *  produce category items       produce category items 
 *  a1      A        10          a2      A        31
 *  a2      A        11          b1      B        17
 *  b1      B        17          b1      B        13
 *  a1      A        11          b1      B        10
 * 
 * This rule expand data for every distinct aggregate function and non-distinct 
 * aggregate functions. Each repeated data will be assign a unique group id.
 * 
 * 
 *       Node 1                          Node 2
 *
 *  produce category items  gid     produce category items  gid 
 *  null    null     10     0       null    null     31     0
 *  null    null     11     0       null    null     17     0
 *  null    null     17     0       null    null     13     0
 *  null    null     11     0       null    null     10     0
 *  a1      null     null   1       a2      null     null   1
 *  a2      null     null   1       b1      null     null   1
 *  b1      null     null   1       b1      null     null   1
 *  a1      null     null   1       b1      null     null   1
 *  null    A        null   2       null    A        null   2
 *  null    A        null   2       null    B        null   2
 *  null    B        null   2       null    B        null   2
 *  null    A        null   2       null    B        null   2
*/
class ExpandStep : public ITransformingStep
{
public:
    static const std::string group_id;
    static const std::string group_id_mask;

    explicit ExpandStep(
        const DataStream & input_stream_,
        Assignments assignments_,
        NameToType name_to_type_,
        String group_id_symbol_,
        std::set<Int32> group_id_value_,
        std::map<Int32, Names> group_id_non_null_symbol_);

    String getName() const override { return "Expand"; }
    Type getType() const override { return Type::Expand; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;
    void toProto(Protos::ExpandStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ExpandStep> fromProto(const Protos::ExpandStep & proto, ContextPtr context);

    const Assignments & getAssignments() const { return assignments; }
    const NameToType & getNameToType() const { return name_to_type; }
    const String & getGroupIdSymbol() const { return group_id_symbol; }
    const std::set<Int32> & getGroupIdValue() const { return group_id_value; }
    const std::map<Int32, Names> & getGroupIdNonNullSymbol() const { return group_id_non_null_symbol; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    std::vector<Assignments> generateAssignmentsGroups() const;
    NamesAndTypesList generateNameTypePreGroup() const;

    void prepare(const PreparedStatementContext & prepared_context) override;

private:
    Assignments assignments;
    NameToType name_to_type;

    /// group id symbol.
    String group_id_symbol;
    /// group id values [0, 1, 2, ...]
    std::set<Int32> group_id_value;

    std::map<Int32, Names> group_id_non_null_symbol;

    static ActionsDAGPtr createActions(const Assignments & assignments, const NamesAndTypesList & source, ContextPtr context);
};

}
