#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>


namespace DB
{

using DatabaseAndTableName = std::pair<String, String>;

struct SourceInfo
{
    std::vector<DatabaseAndTableName> source_tables;
    IndexToNameMap id_to_source_name; // id -> column or expression name
    NameToIndexMap source_name_to_id; // column or expression name -> id

    void add(size_t id, String source_name)
    {
        id_to_source_name.emplace(id, source_name);
        source_name_to_id.emplace(source_name, id);
    }
    bool isEmpty() const
    {
        return id_to_source_name.empty() || source_name_to_id.empty();
    }
    String getNameById(size_t id)
    {
        return id_to_source_name.at(id);
    }
    size_t getIdByName(String name)
    {
        return source_name_to_id.at(name);
    }
    bool contains(String source_name) const
    {
        return source_name_to_id.contains(source_name);
    }
};

using TableToSourceMap = std::unordered_map<std::string, SourceInfo>; // FullTableName -> SourceInfo

struct OutputStreamInfo;
using OutputStreamInfoPtr = std::shared_ptr<OutputStreamInfo>;

struct OutputStreamInfo
{
    String output_name;
    std::unordered_set<size_t> column_ids;

    void add(OutputStreamInfoPtr & info)
    {
        for (const auto & id : info->column_ids)
            column_ids.emplace(id);
    }

    bool isEmpty() const
    {
        return column_ids.empty();
    }
};

struct InsertInfo
{
    String database;
    String table;

    struct InsertColumnInfo
    {
        String insert_column_name;
        String input_column;
    };
    std::vector<InsertColumnInfo> insert_columns_info;
};

struct LineageInfoContext
{
    std::vector<DatabaseAndTableName> tables;
    std::unordered_map<String, OutputStreamInfoPtr> output_stream_lineages;
};

class LineageInfoVisitor : public PlanNodeVisitor<void, LineageInfoContext>
{
public:
    explicit LineageInfoVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_)
    {
        column_id_allocator = std::make_shared<PlanNodeIdAllocator>(0);
    }

private:
    void visitPlanNode(PlanNodeBase & node, LineageInfoContext & lineage_info_context) override;

#define VISITOR_DEF(TYPE) void visit##TYPE##Node(TYPE##Node &, LineageInfoContext &) override;
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

    // template <bool require_all>
    void visitSetOperationNode(PlanNodeBase & node, LineageInfoContext & lineage_info_context);

    void
    visitISourceNodeWithoutStorage(PlanNodeBase & node, LineageInfoContext & lineage_info_context, DatabaseAndTableName source_table = {});

    void addNewExpressionSource(PlanNodeBase & node, LineageInfoContext & lineage_info_context, Names new_header_names);

    ContextMutablePtr context;
    SimpleCTEVisitHelper<void> cte_helper;
    std::unordered_map<CTEId, LineageInfoContext> cte_visit_results;

    PlanNodeIdAllocatorPtr column_id_allocator;

public:
    TableToSourceMap table_sources;
    std::vector<SourceInfo> expression_or_value_sources;
    std::shared_ptr<InsertInfo> insert_info;
};

}
