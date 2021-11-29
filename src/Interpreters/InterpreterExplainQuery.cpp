#include <Interpreters/InterpreterExplainQuery.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageView.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/printPipeline.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
    extern const int LOGICAL_ERROR;
}

namespace
{
    struct ExplainAnalyzedSyntaxMatcher
    {
        struct Data : public WithContext
        {
            explicit Data(ContextPtr context_) : WithContext(context_) {}
        };

        static bool needChildVisit(ASTPtr & node, ASTPtr &)
        {
            return !node->as<ASTSelectQuery>();
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                visit(*select, ast, data);
        }

        static void visit(ASTSelectQuery & select, ASTPtr & node, Data & data)
        {
            InterpreterSelectQuery interpreter(
                node, data.getContext(), SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());

            const SelectQueryInfo & query_info = interpreter.getQueryInfo();
            if (query_info.view_query)
            {
                ASTPtr tmp;
                StorageView::replaceWithSubquery(select, query_info.view_query->clone(), tmp);
            }
        }
    };

    using ExplainAnalyzedSyntaxVisitor = InDepthNodeVisitor<ExplainAnalyzedSyntaxMatcher, true>;

}

BlockIO InterpreterExplainQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterExplainQuery::getSampleBlock()
{
    Block block;
    const auto & ast = query->as<ASTExplainQuery &>();
    if (ast.getKind() == ASTExplainQuery::MaterializedView)
    {
        auto view_table_column = ColumnString::create();
        auto hit_view = ColumnUInt8::create();
        auto match_cost = ColumnInt64::create();
        auto read_cost = ColumnUInt64::create();
        auto view_inner_query = ColumnString::create();
        auto miss_match_reason = ColumnString::create();
        block.insert({std::move(view_table_column), std::make_shared<DataTypeString>(), "view_table"});
        block.insert({std::move(hit_view), std::make_shared<DataTypeUInt8>(), "match_result"});
        block.insert({std::move(match_cost), std::make_shared<DataTypeInt64>(), "match_cost"});
        block.insert({std::move(read_cost), std::make_shared<DataTypeUInt64>(), "read_cost"});
        block.insert({std::move(view_inner_query), std::make_shared<DataTypeString>(), "view_query"});
        block.insert({std::move(miss_match_reason), std::make_shared<DataTypeString>(), "miss_reason"});
    }
    else
    {
        ColumnWithTypeAndName col;
        col.name = "explain";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        block.insert(col);
    }
    return block;
}

/// Split str by line feed and write as separate row to ColumnString.
static void fillColumn(IColumn & column, const std::string & str)
{
    size_t start = 0;
    size_t end = 0;
    size_t size = str.size();

    while (end < size)
    {
        if (str[end] == '\n')
        {
            column.insertData(str.data() + start, end - start);
            start = end + 1;
        }

        ++end;
    }

    if (start < end)
        column.insertData(str.data() + start, end - start);
}

namespace
{

/// Settings. Different for each explain type.

struct QueryPlanSettings
{
    QueryPlan::ExplainPlanOptions query_plan_options;

    /// Apply query plan optimizations.
    bool optimize = true;
    bool json = false;

    constexpr static char name[] = "PLAN";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_plan_options.header},
            {"description", query_plan_options.description},
            {"actions", query_plan_options.actions},
            {"indexes", query_plan_options.indexes},
            {"optimize", optimize},
            {"json", json}
    };
};

struct QueryPipelineSettings
{
    QueryPlan::ExplainPipelineOptions query_pipeline_options;
    bool graph = false;
    bool compact = true;

    constexpr static char name[] = "PIPELINE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_pipeline_options.header},
            {"graph", graph},
            {"compact", compact},
    };
};

template <typename Settings>
struct ExplainSettings : public Settings
{
    using Settings::boolean_settings;

    bool has(const std::string & name_) const
    {
        return boolean_settings.count(name_) > 0;
    }

    void setBooleanSetting(const std::string & name_, bool value)
    {
        auto it = boolean_settings.find(name_);
        if (it == boolean_settings.end())
            throw Exception("Unknown setting for ExplainSettings: " + name_, ErrorCodes::LOGICAL_ERROR);

        it->second.get() = value;
    }

    std::string getSettingsList() const
    {
        std::string res;
        for (const auto & setting : boolean_settings)
        {
            if (!res.empty())
                res += ", ";

            res += setting.first;
        }

        return res;
    }
};

template <typename Settings>
ExplainSettings<Settings> checkAndGetSettings(const ASTPtr & ast_settings)
{
    if (!ast_settings)
        return {};

    ExplainSettings<Settings> settings;
    const auto & set_query = ast_settings->as<ASTSetQuery &>();

    for (const auto & change : set_query.changes)
    {
        if (!settings.has(change.name))
            throw Exception("Unknown setting \"" + change.name + "\" for EXPLAIN " + Settings::name + " query. "
                            "Supported settings: " + settings.getSettingsList(), ErrorCodes::UNKNOWN_SETTING);

        if (change.value.getType() != Field::Types::UInt64)
            throw Exception("Invalid type " + std::string(change.value.getTypeName()) + " for setting \"" + change.name +
                            "\" only boolean settings are supported", ErrorCodes::INVALID_SETTING_VALUE);

        auto value = change.value.get<UInt64>();
        if (value > 1)
            throw Exception("Invalid value " + std::to_string(value) + " for setting \"" + change.name +
                            "\". Only boolean settings are supported", ErrorCodes::INVALID_SETTING_VALUE);

        settings.setBooleanSetting(change.name, value);
    }

    return settings;
}

}

void InterpreterExplainQuery::rewriteDistributedToLocal(ASTPtr & ast)
{
    if (!ast)
        return;

    if (ASTSelectWithUnionQuery * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union->list_of_selects->children)
            rewriteDistributedToLocal(child);
    }
    else if (ASTTableExpression * table = ast->as<ASTTableExpression>())
    {
        if (table->subquery)
            rewriteDistributedToLocal(table->subquery);
        else if (table->database_and_table_name)
        {
            auto target_table_id = getContext()->resolveStorageID(table->database_and_table_name);
            auto storage = DatabaseCatalog::instance().getTable(target_table_id, getContext());
            auto * distributed = dynamic_cast<StorageDistributed *>(storage.get());

            if (!distributed)
                return;

            String table_alias = table->database_and_table_name->tryGetAlias();
            auto old_table_ast = std::find(table->children.begin(), table->children.end(), table->database_and_table_name);
            table->database_and_table_name = std::make_shared<ASTTableIdentifier>(distributed->getRemoteDatabaseName(), distributed->getRemoteTableName());
            *old_table_ast = table->database_and_table_name;

            if (!table_alias.empty())
                table->database_and_table_name->setAlias(table_alias);
        }
    }
    else
    {
        for (auto & child : ast->children)
            rewriteDistributedToLocal(child);
    }
}

BlockInputStreamPtr InterpreterExplainQuery::executeImpl()
{
    const auto & ast = query->as<ASTExplainQuery &>();

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    WriteBufferFromOwnString buf;
    bool single_line = false;

    if (ast.getKind() == ASTExplainQuery::ParsedAST)
    {
        if (ast.getSettings())
            throw Exception("Settings are not supported for EXPLAIN AST query.", ErrorCodes::UNKNOWN_SETTING);

        dumpAST(*ast.getExplainedQuery(), buf);
    }
    else if (ast.getKind() == ASTExplainQuery::AnalyzedSyntax)
    {
        if (ast.getSettings())
            throw Exception("Settings are not supported for EXPLAIN SYNTAX query.", ErrorCodes::UNKNOWN_SETTING);

        ExplainAnalyzedSyntaxVisitor::Data data(getContext());
        ExplainAnalyzedSyntaxVisitor(data).visit(query);

        ast.getExplainedQuery()->format(IAST::FormatSettings(buf, false));
    }
    else if (ast.getKind() == ASTExplainQuery::QueryPlan)
    {
        if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            throw Exception("Only SELECT is supported for EXPLAIN query", ErrorCodes::INCORRECT_QUERY);

        auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
        QueryPlan plan;

        InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), SelectQueryOptions());
        interpreter.buildQueryPlan(plan);

        if (settings.optimize)
            plan.optimize(QueryPlanOptimizationSettings::fromContext(getContext()));

        if (settings.json)
        {
            /// Add extra layers to make plan look more like from postgres.
            auto plan_map = std::make_unique<JSONBuilder::JSONMap>();
            plan_map->add("Plan", plan.explainPlan(settings.query_plan_options));
            auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
            plan_array->add(std::move(plan_map));

            auto format_settings = getFormatSettings(getContext());
            format_settings.json.quote_64bit_integers = false;

            JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
            JSONBuilder::FormatContext format_context{.out = buf};

            plan_array->format(json_format_settings, format_context);

            single_line = true;
        }
        else
            plan.explainPlan(buf, settings.query_plan_options);
    }
    else if (ast.getKind() == ASTExplainQuery::QueryPipeline)
    {
        if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            throw Exception("Only SELECT is supported for EXPLAIN query", ErrorCodes::INCORRECT_QUERY);

        auto settings = checkAndGetSettings<QueryPipelineSettings>(ast.getSettings());
        QueryPlan plan;

        InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), SelectQueryOptions());
        interpreter.buildQueryPlan(plan);
        auto pipeline = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(getContext()),
            BuildQueryPipelineSettings::fromContext(getContext()));

        if (settings.graph)
        {
            /// Pipe holds QueryPlan, should not go out-of-scope
            auto pipe = QueryPipeline::getPipe(std::move(*pipeline));
            const auto & processors = pipe.getProcessors();

            if (settings.compact)
                printPipelineCompact(processors, buf, settings.query_pipeline_options.header);
            else
                printPipeline(processors, buf);
        }
        else
        {
            plan.explainPipeline(buf, settings.query_pipeline_options);
        }
    }
    else if (ast.getKind() == ASTExplainQuery::MaterializedView)
    {
        /// rewrite distributed table to local table
        ASTPtr explain_query = ast.getExplainedQuery()->clone();
        rewriteDistributedToLocal(explain_query);

        ASTSelectWithUnionQuery * select_union_query = explain_query->as<ASTSelectWithUnionQuery>();
        if (select_union_query && !select_union_query->list_of_selects->children.empty())
        {
            Block block;
            auto view_table_column = ColumnString::create();
            auto hit_view = ColumnUInt8::create();
            auto match_cost = ColumnInt64::create();
            auto read_cost = ColumnUInt64::create();
            auto view_inner_query = ColumnString::create();
            auto miss_match_reason = ColumnString::create();

            /// Enable enable_view_based_query_rewrite setting to get view match result
            ContextMutablePtr context = Context::createCopy(getContext());
            context->setSetting("enable_view_based_query_rewrite", 1);
            for (const auto & element : select_union_query->list_of_selects->children)
            {
                InterpreterSelectQuery interpreter{element, context, SelectQueryOptions(QueryProcessingStage::FetchColumns)};
                std::shared_ptr<const MaterializedViewOptimizerResult> mv_optimizer_result = interpreter.getMaterializeViewMatchResult();
                int MAX = std::numeric_limits<int>::max();
                if (!mv_optimizer_result->validate_info.empty())
                {
                    view_table_column->insertDefault();
                    hit_view->insert(0);
                    match_cost->insertDefault();
                    read_cost->insert(0);
                    view_inner_query->insertDefault();
                    miss_match_reason->insert(mv_optimizer_result->validate_info);
                }
                else
                {
                    for (auto iter = mv_optimizer_result->views_match_info.begin(); iter != mv_optimizer_result->views_match_info.end();
                         iter++)
                    {
                        MatchResult::ResultPtr result = *iter;
                        if (!result->view_table)
                            continue;

                        /// insert view name
                        view_table_column->insert(result->view_table->getStorageID().getFullNameNotQuoted());

                        /// insert view match result
                        if (result->cost != MAX && iter == mv_optimizer_result->views_match_info.begin())
                            hit_view->insert(1);
                        else
                            hit_view->insert(0);

                        /// insert cost value
                        match_cost->insert(result->cost);

                        /// insert read cost
                        read_cost->insert(result->read_cost);

                        /// insert view inner query
                        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(result->view_table.get());
                        if (materialized_view)
                        {
                            ASTPtr view_query = materialized_view->getInnerQuery();
                            view_inner_query->insert(queryToString(view_query));
                        }
                        else
                            view_inner_query->insertDefault();

                        miss_match_reason->insert(result->view_match_info);
                    }
                }
            }
            block.insert({std::move(view_table_column), std::make_shared<DataTypeString>(), "view_table"});
            block.insert({std::move(hit_view), std::make_shared<DataTypeUInt8>(), "match_result"});
            block.insert({std::move(match_cost), std::make_shared<DataTypeInt64>(), "match_cost"});
            block.insert({std::move(read_cost), std::make_shared<DataTypeUInt64>(), "read_cost"});
            block.insert({std::move(view_inner_query), std::make_shared<DataTypeString>(), "view_query"});
            block.insert({std::move(miss_match_reason), std::make_shared<DataTypeString>(), "miss_reason"});
            return std::make_shared<OneBlockInputStream>(block);
        }
    }

    if (single_line)
        res_columns[0]->insertData(buf.str().data(), buf.str().size());
    else
        fillColumn(*res_columns[0], buf.str());

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}
