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

#include <sstream>
#include <unordered_map>
#include <Access/ContextAccess.h>
#include <Analyzers/ExprAnalyzer.h>
#include <Analyzers/ExpressionVisitor.h>
#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/ScopeAwareEquals.h>
#include <Analyzers/analyze_common.h>
#include <Analyzers/function_utils.h>
#include <Analyzers/resolveNamesAsMySQL.h>
#include <Analyzers/tryEvaluateConstantExpression.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/join_common.h>
#include <Interpreters/processColumnTransformers.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFieldReference.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <QueryPlan/Void.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMemory.h>

#include <sstream>
#include <unordered_map>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/join_common.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Optimizer/Utils.h>
#include <Storages/MergeTree/MergeTreeMeta.h>
#include <Storages/RemoteFile/IStorageCnchFile.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include "Parsers/formatAST.h"

#include <common/logger_useful.h>

using namespace std::string_literals;

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int EXPECTED_ALL_OR_ANY;
    extern const int ILLEGAL_AGGREGATION;
    extern const int NOT_AN_AGGREGATE;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int UNKNOWN_JOIN;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int EMPTY_NESTED_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_PREWHERE;
    extern const int ACCESS_DENIED;
}

class QueryAnalyzerVisitor : public ASTVisitor<Void, const Void>
{
public:
    Void process(ASTPtr & node) { return ASTVisitorUtil::accept(node, *this, {}); }

    Void visitASTInsertQuery(ASTPtr & node, const Void &) override;
    Void visitASTSelectIntersectExceptQuery(ASTPtr & node, const Void &) override;
    Void visitASTSelectWithUnionQuery(ASTPtr & node, const Void &) override;
    Void visitASTSelectQuery(ASTPtr & node, const Void &) override;
    Void visitASTSubquery(ASTPtr & node, const Void &) override;
    Void visitASTExplainQuery(ASTPtr & node, const Void &) override;
    Void visitASTCreatePreparedStatementQuery(ASTPtr & node, const Void &) override
    {
        auto & prepare = node->as<ASTCreatePreparedStatementQuery &>();
        auto query = prepare.getQuery();
        process(query);
        analysis.setOutputDescription(*node, analysis.getOutputDescription(*query));
        return {};
    }

    QueryAnalyzerVisitor(ContextPtr context_, Analysis & analysis_, ScopePtr outer_query_scope_)
        : ASTVisitor(context_->getSettingsRef().max_ast_depth)
        , context(std::move(context_))
        , analysis(analysis_)
        , outer_query_scope(outer_query_scope_)
        , use_ansi_semantic(context->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
        , enable_shared_cte(context->getSettingsRef().cte_mode != CTEMode::INLINED)
        , enable_implicit_type_conversion(context->getSettingsRef().enable_implicit_type_conversion)
        , allow_extended_conversion(context->getSettingsRef().allow_extended_type_conversion)
        , enable_subcolumn_optimization_through_union(context->getSettingsRef().enable_subcolumn_optimization_through_union)
        , enable_implicit_arg_type_convert(context->getSettingsRef().enable_implicit_arg_type_convert)
    {
    }

private:
    ContextPtr context;
    Analysis & analysis;
    const ScopePtr outer_query_scope;
    const bool use_ansi_semantic;
    const bool enable_shared_cte;
    const bool enable_implicit_type_conversion;
    const bool allow_extended_conversion;
    const bool enable_subcolumn_optimization_through_union;
    const bool enable_implicit_arg_type_convert; // MySQL implicit cast rules

    LoggerPtr logger = getLogger("QueryAnalyzerVisitor");

    void analyzeSetOperation(ASTPtr & node, ASTs & selects);

    /// FROM clause
    ScopePtr analyzeWithoutFrom(ASTSelectQuery & select_query);
    ScopePtr analyzeFrom(ASTTablesInSelectQuery & tables_in_select, ASTSelectQuery & select_query, const Aliases & query_aliases);
    ScopePtr analyzeTableExpression(
        ASTTableExpression & table_expression,
        const QualifiedName & column_prefix,
        ASTSelectQuery & select_query,
        const Aliases & query_aliases);
    ScopePtr analyzeTable(
        ASTTableIdentifier & db_and_table,
        const QualifiedName & column_prefix,
        ASTSelectQuery & select_query,
        const Aliases & query_aliases);
    ScopePtr analyzeSubquery(ASTPtr & node, const QualifiedName & column_prefix);
    ScopePtr analyzeTableFunction(ASTFunction & table_function, const QualifiedName & column_prefix);
    ScopePtr analyzeJoin(
        ASTTableJoin & table_join,
        ScopePtr left_scope,
        ScopePtr right_scope,
        const String & right_table_qualifier,
        ASTSelectQuery & select_query);
    ScopePtr analyzeJoinUsing(
        ASTTableJoin & table_join,
        ScopePtr left_scope,
        ScopePtr right_scope,
        const String & right_table_qualifier,
        ASTSelectQuery & select_query);
    ScopePtr analyzeJoinOn(ASTTableJoin & table_join, ScopePtr left_scope, ScopePtr right_scope, const String & right_table_qualifier);
    ScopePtr analyzeArrayJoin(ASTArrayJoin & array_join, ASTSelectQuery & select_query, ScopePtr source_scope);

    void analyzeWindow(ASTSelectQuery & select_query);
    void analyzePrewhere(ASTSelectQuery & select_query, ScopePtr source_scope, ASTPtr & alias_columns, const Aliases & query_aliases);
    void analyzeWhere(ASTSelectQuery & select_query, ScopePtr source_scope);
    ASTs analyzeSelect(ASTSelectQuery & select_query, ScopePtr source_scope);
    void analyzeGroupBy(ASTSelectQuery & select_query, ASTs & select_expressions, ScopePtr source_scope);
    // void analyzeInterestEvents(ASTSelectQuery & select_query);
    void analyzeHaving(ASTSelectQuery & select_query, ScopePtr source_scope);
    void analyzeOrderBy(ASTSelectQuery & select_query, ASTs & select_expressions, ScopePtr output_scope);
    void analyzeLimitBy(ASTSelectQuery & select_query, ASTs & select_expressions, ScopePtr output_scope);
    void analyzeLimitAndOffset(ASTSelectQuery & select_query);

    void analyzeOutfile(ASTSelectWithUnionQuery & outfile_query);

    /// utils
    ScopePtr createScope(FieldDescriptions field_descriptions, ScopePtr parent = nullptr);
    DatabaseAndTableWithAlias extractTableWithAlias(const ASTTableExpression & table_expression);
    void verifyNoAggregateWindowOrGroupingOperations(ASTPtr & expression, const String & statement_name);
    void verifyAggregate(ASTSelectQuery & select_query, ScopePtr source_scope);
    void verifyNoFreeReferencesToLambdaArgument(ASTSelectQuery & select_query);
    UInt64 analyzeUIntConstExpression(const ASTPtr & expression);
    void countLeadingHint(const IAST & ast);
    void rewriteSelectInANSIMode(ASTSelectQuery & select_query, const Aliases & aliases, ScopePtr source_scope);
    void normalizeAliases(ASTPtr & expr, ASTPtr & aliases_expr);
    void normalizeAliases(ASTPtr & expr, const Aliases & aliases, const NameSet & source_columns_set);
};

static NameSet collectNames(ScopePtr scope);
static String
qualifyJoinedName(const String & name, const String & table_qualifier, const NameSet & source_names, bool check_identifier_begin_valid);

static void checkAccess(AnalysisPtr analysis, ContextPtr context);

AnalysisPtr QueryAnalyzer::analyze(ASTPtr & ast, ContextPtr context)
{
    AnalysisPtr analysis_ptr = std::make_unique<Analysis>();
    analyze(ast, nullptr, context, *analysis_ptr);
    checkAccess(analysis_ptr, context);
    return analysis_ptr;
}

void QueryAnalyzer::analyze(ASTPtr & query, ScopePtr outer_query_scope, ContextPtr context, Analysis & analysis)
{
    QueryAnalyzerVisitor analyzer_visitor{std::move(context), analysis, outer_query_scope};
    analyzer_visitor.process(query);
}

Void QueryAnalyzerVisitor::visitASTInsertQuery(ASTPtr & node, const Void &)
{
    auto & insert_query = node->as<ASTInsertQuery &>();
    if (insert_query.table_id.database_name.empty())
        insert_query.table_id.database_name = context->getCurrentDatabase();
    StoragePtr storage = DatabaseCatalog::instance().getTable(insert_query.table_id, context);
    // if (auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(table.get()))
    //     throw Exception("Inserting into materialized views is not supported", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    // if (auto * view = dynamic_cast<const StorageView *>(table.get()))
    //     throw Exception("Inserting into views is not supported", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    StorageMetadataPtr storage_metadata = storage->getInMemoryMetadataPtr();

    // For StorageCnchMergeTree, the metadata of distributed table may diff with the metadata of local table.
    // In this case, we use the one of local table.
    // if (const auto * storage_distributed = dynamic_cast<const StorageCnchMergeTree *>(storage.get()))
    // {
    //     // when diff server diff remote database name, the database name is empty.
    //     if (!storage_distributed->getRemoteDatabaseName().empty())
    //     {
    //         StorageID local_id {storage_distributed->getRemoteDatabaseName(), storage_distributed->getRemoteTableName()};
    //         auto storage_local = DatabaseCatalog::instance().getTable(local_id, context);
    //         storage_metadata = storage_local->getInMemoryMetadataPtr();
    //     }
    // }

    auto table_columns = storage_metadata->getColumns();
    NamesAndTypes insert_columns;
    std::unordered_set<String> insert_columns_set;
    if (insert_query.columns)
    {
        const auto columns_ast = processColumnTransformers(context->getCurrentDatabase(), storage, storage_metadata, insert_query.columns);

        for (auto & column_ast : columns_ast->children)
        {
            const auto & column_name = column_ast->as<ASTIdentifier &>().name();
            if (!insert_columns_set.emplace(column_name).second)
                throw Exception("Duplicate column " + column_name + " in INSERT query", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            auto column = table_columns.tryGetPhysical(column_name);
            if (!column)
            {
                auto func_column = storage_metadata->getFuncColumns().tryGetByName(column_name);
                if (func_column)
                    column = func_column;
                else
                    throw Exception(
                        fmt::format("Cannot find column {} when insert-select on optimizer mode", column_name), ErrorCodes::ILLEGAL_COLUMN);
            }
            insert_columns.emplace_back(NameAndTypePair{(*column).name, (*column).type});
        }
    }
    else
    {
        for (auto & column : table_columns.getAllPhysical())
            insert_columns.emplace_back(NameAndTypePair{column.name, column.type});
    }

    // check column mask & row filter?
    if (!insert_query.select)
        throw Exception("Only support insert select", ErrorCodes::NOT_IMPLEMENTED);

    process(insert_query.select);
    auto query_columns = analysis.getOutputDescription(*insert_query.select);
    if (query_columns.size() != insert_columns.size())
        throw Exception(
            "Insert query has mismatched column size, Table: " + std::to_string(insert_columns.size())
                + ", Query: " + std::to_string(query_columns.size()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    NamesAndTypes inserted_query_columns;
    for (const auto & item : query_columns)
    {
        inserted_query_columns.emplace_back(NameAndTypePair{item.name, item.type});
    }

    analysis.insert_analysis = InsertAnalysis{storage, insert_query.table_id, insert_columns};
    analysis.output_descriptions[node.get()] = {};
    return {};
}

Void QueryAnalyzerVisitor::visitASTSelectIntersectExceptQuery(ASTPtr & node, const Void &)
{
    auto & intersect_or_except = node->as<ASTSelectIntersectExceptQuery &>();
    auto list_of_selects = intersect_or_except.getListOfSelects();

    assert(intersect_or_except.final_operator != ASTSelectIntersectExceptQuery::Operator::UNKNOWN);
    analyzeSetOperation(node, list_of_selects);
    return {};
}

Void QueryAnalyzerVisitor::visitASTSelectWithUnionQuery(ASTPtr & node, const Void &)
{
    auto & select_with_union = node->as<ASTSelectWithUnionQuery &>();

    // some ASTSelectWithUnionQueries are generated by QueryRewriter,
    // thus are not normalized. see also: JoinToSubqueryTransformVisitor.cpp
    bool normalized = (select_with_union.is_normalized
                       && (select_with_union.union_mode == ASTSelectWithUnionQuery::Mode::UNION_ALL
                           || select_with_union.union_mode == ASTSelectWithUnionQuery::Mode::UNION_DISTINCT))
        || select_with_union.list_of_selects->children.size() == 1;
    if (!normalized)
        throw Exception("Invalid ASTSelectWithUnionQuery found", ErrorCodes::LOGICAL_ERROR);

    analyzeSetOperation(node, select_with_union.list_of_selects->children);
    analyzeOutfile(select_with_union);
    return {};
}

Void QueryAnalyzerVisitor::visitASTSelectQuery(ASTPtr & node, const Void &)
{
    auto & select_query = node->as<ASTSelectQuery &>();
    ScopePtr source_scope;

    // Collect query aliases for aliases rewritting, for ANSI only.
    // since aliases rewritting for CLICKHOUSE is done by QueryRewriter
    Aliases query_aliases;
    if (use_ansi_semantic)
        QueryAliasesAllowAmbiguousNoSubqueriesVisitor(query_aliases).visit(node);

    if (select_query.tables())
        source_scope = analyzeFrom(select_query.refTables()->as<ASTTablesInSelectQuery &>(), select_query, query_aliases);
    else
        source_scope = analyzeWithoutFrom(select_query);

    rewriteSelectInANSIMode(select_query, query_aliases, source_scope);
    analyzeWindow(select_query);
    analyzeWhere(select_query, source_scope);
    // analyze SELECT first since SELECT item may be referred in GROUP BY/ORDER BY
    ASTs select_expression = analyzeSelect(select_query, source_scope);
    analyzeGroupBy(select_query, select_expression, source_scope);
    // analyzeInterestEvents(select_query);
    analyzeHaving(select_query, source_scope);
    analyzeOrderBy(select_query, select_expression, source_scope);
    analyzeLimitBy(select_query, select_expression, source_scope);
    analyzeLimitAndOffset(select_query);
    verifyAggregate(select_query, source_scope);
    verifyNoFreeReferencesToLambdaArgument(select_query);
    countLeadingHint(select_query);
    return {};
}

Void QueryAnalyzerVisitor::visitASTSubquery(ASTPtr & node, const Void &)
{
    auto & subquery = node->as<ASTSubquery &>();

    if (enable_shared_cte && subquery.isWithClause())
    {
        analysis.registerCTE(subquery);

        // CTE has been analyzed
        if (auto cte_analysis = analysis.tryGetCTEAnalysis(subquery); cte_analysis->isSharable())
        {
            auto * representative = cte_analysis->representative;
            analysis.setOutputDescription(subquery, analysis.getOutputDescription(*representative));
            return {};
        }
    }

    process(node->children.front());
    return {};
}

Void QueryAnalyzerVisitor::visitASTExplainQuery(ASTPtr & node, const Void &)
{
    ASTExplainQuery & explain = node->as<ASTExplainQuery &>();
    if (explain.getKind() != ASTExplainQuery::ExplainKind::LogicalAnalyze
        && explain.getKind() != ASTExplainQuery::ExplainKind::DistributedAnalyze
        && explain.getKind() != ASTExplainQuery::ExplainKind::PipelineAnalyze)
        throw Exception("Unexpected explain kind", ErrorCodes::LOGICAL_ERROR);
    if (!explain.getExplainedQuery())
        throw Exception("Explain query has syntax error ", ErrorCodes::LOGICAL_ERROR);

    process(explain.getExplainedQuery());
    FieldDescriptions output_description;
    output_description.push_back({"Explain Analyze", std::make_shared<DataTypeString>()});
    analysis.setOutputDescription(explain, output_description);
    return {};
}

void QueryAnalyzerVisitor::analyzeSetOperation(ASTPtr & node, ASTs & selects)
{
    size_t column_size = 0;

    // analyze union elements
    for (auto & select : selects)
    {
        process(select);

        if (column_size == 0)
            column_size = analysis.getOutputDescription(*select).size();
        else if (column_size != analysis.getOutputDescription(*select).size())
            throw Exception(
                "Different number of columns for UNION ALL elements: " + serializeAST(*node),
                ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);
    }

    // analyze output fields
    FieldDescriptions output_desc;
    if (selects.size() == 1)
    {
        output_desc = analysis.getOutputDescription(*selects[0]);
    }
    else
    {
        auto & first_input_desc = analysis.getOutputDescription(*selects[0]);

        for (size_t column_idx = 0; column_idx < column_size; ++column_idx)
        {
            DataTypes elem_types;
            FieldDescription::OriginColumns origin_columns;

            for (auto & select : selects)
            {
                const auto & elem_field = analysis.getOutputDescription(*select)[column_idx];
                elem_types.push_back(elem_field.type);

                if (enable_subcolumn_optimization_through_union)
                    origin_columns.insert(origin_columns.end(), elem_field.origin_columns.begin(), elem_field.origin_columns.end());
            }

            DataTypePtr output_type;
            // promote output type to super type if necessary
            output_type = getCommonType(elem_types, enable_implicit_arg_type_convert, allow_extended_conversion);
            output_desc.emplace_back(
                first_input_desc[column_idx].name,
                output_type,
                /* prefix*/ QualifiedName{},
                std::move(origin_columns),
                /* substituted_by_asterisk */ true,
                /* can_be_array_joined */ true);
        }
    }

    // record type coercion
    if (selects.size() != 1)
    {
        for (auto & select : selects)
        {
            auto & input_desc = analysis.getOutputDescription(*select);
            DataTypes type_coercion(column_size, nullptr);

            for (size_t column_idx = 0; column_idx < column_size; ++column_idx)
            {
                auto input_type = input_desc[column_idx].type;
                auto output_type = output_desc[column_idx].type;

                if (!input_type->equals(*output_type))
                {
                    if (enable_implicit_type_conversion)
                    {
                        type_coercion[column_idx] = output_type;
                    }
                    else
                    {
                        std::ostringstream errormsg;
                        errormsg << "Column type mismatch for UNION: " << serializeAST(*node) << ", expect type: " << output_type->getName()
                                 << ", but found type: " << input_type->getName();
                        throw Exception(errormsg.str(), ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);
                    }
                }
            }

            analysis.setRelationTypeCoercion(*select, type_coercion);
        }
    }

    analysis.setOutputDescription(*node, output_desc);
}

ScopePtr QueryAnalyzerVisitor::analyzeWithoutFrom(ASTSelectQuery & select_query)
{
    FieldDescriptions fields;
    fields.emplace_back("dummy", std::make_shared<DataTypeUInt8>());
    const auto * scope = createScope(fields);
    analysis.setQueryWithoutFromScope(select_query, scope);
    return scope;
}

ScopePtr
QueryAnalyzerVisitor::analyzeFrom(ASTTablesInSelectQuery & tables_in_select, ASTSelectQuery & select_query, const Aliases & query_aliases)
{
    if (tables_in_select.children.empty())
        throw Exception("ASTTableInSelectQuery can not be empty.", ErrorCodes::LOGICAL_ERROR);

    auto analyze_first_table_element = [&](ASTPtr & ast) -> ScopePtr {
        auto & table_element = ast->as<ASTTablesInSelectQueryElement &>();

        if (table_element.table_expression)
        {
            auto * table_expression = table_element.table_expression->as<ASTTableExpression>();
            if (!table_expression)
                throw Exception("Invalid Table Expression", ErrorCodes::LOGICAL_ERROR);
            auto table_with_alias = extractTableWithAlias(*table_expression);
            return analyzeTableExpression(
                *table_expression, QualifiedName::extractQualifiedName(table_with_alias), select_query, query_aliases);
        }
        else
        {
            throw Exception("Invalid table table_element.", ErrorCodes::LOGICAL_ERROR);
        }
    };

    ScopePtr current_scope = analyze_first_table_element(tables_in_select.children[0]);

    for (size_t idx = 1; idx < tables_in_select.children.size(); ++idx)
    {
        auto & table_element = tables_in_select.children[idx]->as<ASTTablesInSelectQueryElement &>();
        if (table_element.table_expression)
        {
            auto * table_expression = table_element.table_expression->as<ASTTableExpression>();
            if (!table_expression)
                throw Exception("Invalid Table Expression", ErrorCodes::LOGICAL_ERROR);
            auto table_with_alias = extractTableWithAlias(*table_expression);
            ScopePtr joined_table_scope = analyzeTableExpression(
                *table_expression, QualifiedName::extractQualifiedName(table_with_alias), select_query, query_aliases);
            auto & table_join = table_element.table_join->as<ASTTableJoin &>();
            current_scope
                = analyzeJoin(table_join, current_scope, joined_table_scope, table_with_alias.getQualifiedNamePrefix(true), select_query);
        }
        else if (table_element.array_join)
        {
            current_scope = analyzeArrayJoin(table_element.array_join->as<ASTArrayJoin &>(), select_query, current_scope);
        }
        else
            throw Exception("Invalid table element.", ErrorCodes::LOGICAL_ERROR);
    }

    countLeadingHint(tables_in_select);
    analysis.setScope(tables_in_select, current_scope);
    return current_scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeTableExpression(
    ASTTableExpression & table_expression,
    const QualifiedName & column_prefix,
    ASTSelectQuery & select_query,
    const Aliases & query_aliases)
{
    ScopePtr scope;

    if (table_expression.database_and_table_name)
        scope = analyzeTable(
            table_expression.database_and_table_name->as<ASTTableIdentifier &>(), column_prefix, select_query, query_aliases);
    else if (table_expression.subquery)
        scope = analyzeSubquery(table_expression.subquery, column_prefix);
    else if (table_expression.table_function)
        scope = analyzeTableFunction(table_expression.table_function->as<ASTFunction &>(), column_prefix);
    else
        throw Exception("Invalid ASTTableExpression: " + serializeAST(table_expression), ErrorCodes::LOGICAL_ERROR);

    countLeadingHint(table_expression);
    return scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeTable(
    ASTTableIdentifier & db_and_table, const QualifiedName & column_prefix, ASTSelectQuery & select_query, const Aliases & query_aliases)
{
    // get storage information
    StoragePtr storage;
    String full_table_name;

    {
        auto storage_id = context->tryResolveStorageID(db_and_table.getTableId());
        storage = DatabaseCatalog::instance().getTable(storage_id, context);
        if (!storage_id.hasUUID())
        {
            storage_id.uuid = storage->getStorageUUID();
        }
        storage->renameInMemory(storage_id);
        full_table_name = storage_id.getFullTableName();

        if (storage_id.getDatabaseName() != "system" && !storage->supportsOptimizer())
            throw Exception("table is not supported in optimizer", ErrorCodes::NOT_IMPLEMENTED);

        analysis.storage_results[&db_and_table] = StorageAnalysis{storage_id.getDatabaseName(), storage_id.getTableName(), storage};
    }

    StorageMetadataPtr metadata_snapshot = storage->getInMemoryMetadataPtr();

    // For StorageDistributed, the metadata of distributed table may diff with the metadata of local table.
    // In this case, we use the one of local table.
    if (const auto * storage_distributed = dynamic_cast<const StorageDistributed *>(storage.get()))
    {
        // when diff server diff remote database name, the database name is empty.
        if (!storage_distributed->getRemoteDatabaseName().empty())
        {
            StorageID local_id{storage_distributed->getRemoteDatabaseName(), storage_distributed->getRemoteTableName()};
            auto storage_local = DatabaseCatalog::instance().getTable(local_id, context);
            metadata_snapshot = storage_local->getInMemoryMetadataPtr();
        }
    }

    auto storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);
    const auto & columns_description = metadata_snapshot->getColumns();
    ScopePtr scope;
    FieldDescriptions fields;
    ASTIdentifier * origin_table_ast = &db_and_table;

    auto add_field = [&](const String & name, const DataTypePtr & type, bool substitude_for_asterisk, bool can_be_array_joined) {
        fields.emplace_back(
            name,
            type,
            column_prefix,
            storage,
            metadata_snapshot,
            origin_table_ast,
            name,
            fields.size(),
            substitude_for_asterisk,
            can_be_array_joined);
    };

    // get columns
    {
        auto get_columns_options = GetColumnsOptions(GetColumnsOptions::All);
        if (storage->supportsSubcolumns())
            get_columns_options.withSubcolumns();
        if (storage->supportsDynamicSubcolumns())
            get_columns_options.withExtendedObjects();

        Names all_column_names;
        for (const auto & column : storage_snapshot->getColumns(get_columns_options))
            all_column_names.emplace_back(column.name);

        Block type_provider = storage_snapshot->getSampleBlockForColumns(all_column_names, {});

        for (const auto & column : columns_description.getOrdinary())
        {
            LOG_TRACE(logger, "analyze table {}, add ordinary field {}", full_table_name, column.name);
            add_field(column.name, type_provider.getByName(column.name).type, true, true);
        }

        for (const auto & column : columns_description.getMaterialized())
        {
            LOG_TRACE(logger, "analyze table {}, add materialized field {}", full_table_name, column.name);
            add_field(column.name, type_provider.getByName(column.name).type, false, true);
        }

        for (const auto & column : storage->getVirtuals())
        {
            LOG_TRACE(logger, "analyze table {}, add virtual field {}", full_table_name, column.name);
            add_field(column.name, column.type, false, true);
        }

        if (storage->supportsSubcolumns())
        {
            for (const auto & column : columns_description.getSubcolumnsOfAllPhysical())
            {
                LOG_TRACE(logger, "analyze table {}, add subcolumn field {}", full_table_name, column.name);
                add_field(column.name, type_provider.getByName(column.name).type, false, false);
            }
        }

        if (storage->supportsDynamicSubcolumns())
        {
            for (const auto & column : storage_snapshot->getSubcolumnsOfObjectColumns())
            {
                LOG_TRACE(logger, "analyze table {}, add dynamic subcolumn field {}", full_table_name, column.name);
                add_field(column.name, type_provider.getByName(column.name).type, false, true);
            }
        }

        scope = createScope(fields);
        analysis.setTableStorageScope(db_and_table, scope);
    }

    // get alias columns
    {
        ASTPtr alias_columns = std::make_shared<ASTExpressionList>();
        for (const auto & column : columns_description.getAliases())
        {
            const auto column_default = columns_description.getDefault(column.name);
            if (column_default)
            {
                alias_columns->children.emplace_back(setAlias(column_default->expression->clone(), column.name));
            }
            else
            {
                throw Exception("Alias column " + column.name + " not found for table " + full_table_name, ErrorCodes::LOGICAL_ERROR);
            }
        }

        // users are allowed to define alias columns like: CREATE TABLE t (a Int32, b ALIAS a + 1, c ALIAS b + 1)
        if (!alias_columns->children.empty())
            normalizeAliases(alias_columns, alias_columns);

        analyzePrewhere(select_query, scope, alias_columns, query_aliases);

        ExprAnalyzerOptions options{"alias column"s};
        options.recordUsedObject(false);
        for (auto & alias_col : alias_columns->children)
        {
            auto col_type = ExprAnalyzer::analyze(alias_col, scope, context, analysis, options);
            auto col_name = alias_col->tryGetAlias();
            LOG_TRACE(logger, "analyze table {}, add alias field {}", full_table_name, col_name);
            add_field(col_name, col_type, false, true);
        }

        scope = createScope(fields);
        analysis.setScope(db_and_table, scope);
        analysis.setTableAliasColumns(db_and_table, std::move(alias_columns->children));
    }

    return scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeSubquery(ASTPtr & node, const QualifiedName & column_prefix)
{
    process(node);

    auto & subquery = node->as<ASTSubquery &>();
    FieldDescriptions field_descriptions;
    for (auto & col : analysis.getOutputDescription(subquery))
    {
        field_descriptions.emplace_back(col.withNewPrefix(column_prefix));
    }

    const auto * subquery_scope = createScope(field_descriptions);
    analysis.setScope(subquery, subquery_scope);
    return subquery_scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeTableFunction(ASTFunction & table_function, const QualifiedName & column_prefix)
{
    // execute table function
    StoragePtr storage = context->getQueryContext()->executeTableFunction(table_function.ptr());
    StorageID storage_id = storage->getStorageID();
    analysis.storage_results[&table_function]
        = StorageAnalysis{.database = storage_id.getDatabaseName(), .table = storage_id.getTableName(), .storage = storage};

    // get columns information
    auto storage_metadata = storage->getInMemoryMetadataPtr();
    const auto & columns_description = storage_metadata->getColumns();
    FieldDescriptions field_descriptions;

    for (const auto & column : columns_description.getAllPhysical())
    {
        field_descriptions.emplace_back(
            column.name,
            column.type,
            column_prefix,
            storage,
            storage_metadata,
            &table_function,
            column.name,
            field_descriptions.size(),
            true,
            true);
    }

    const auto * table_function_scope = createScope(field_descriptions);
    analysis.setScope(table_function, table_function_scope);
    return table_function_scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeJoin(
    ASTTableJoin & table_join,
    ScopePtr left_scope,
    ScopePtr right_scope,
    const String & right_table_qualifier,
    ASTSelectQuery & select_query)
{
    // set join strictness if unspecified
    {
        auto join_default_strictness = context->getSettingsRef().join_default_strictness;
        if (table_join.strictness == ASTTableJoin::Strictness::Unspecified && !isCrossJoin(table_join))
        {
            if (join_default_strictness == JoinStrictness::ANY)
                table_join.strictness = ASTTableJoin::Strictness::Any;
            else if (join_default_strictness == JoinStrictness::ALL)
                table_join.strictness = ASTTableJoin::Strictness::All;
            else
                throw Exception(
                    "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty",
                    DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
        }
    }

    if (context->getSettingsRef().any_join_distinct_right_table_keys)
    {
        if (table_join.strictness == ASTTableJoin::Strictness::Any && table_join.kind == ASTTableJoin::Kind::Inner)
        {
            table_join.strictness = ASTTableJoin::Strictness::Semi;
            table_join.kind = ASTTableJoin::Kind::Left;
        }

        if (table_join.strictness == ASTTableJoin::Strictness::Any)
            table_join.strictness = ASTTableJoin::Strictness::RightAny;
    }

    {
        if (isCrossJoin(table_join))
        {
            if (table_join.strictness != ASTTableJoin::Strictness::Unspecified)
                throw Exception("CROSS/COMMA join must leave join strictness unspecified.", ErrorCodes::UNKNOWN_JOIN);

            if (joinCondition(table_join))
                throw Exception("CROSS/COMMA join must not have join conditions.", ErrorCodes::UNKNOWN_JOIN);
        }
        else if (isSemiOrAntiJoin(table_join))
        {
            if (!isInner(table_join.kind) && !isLeft(table_join.kind) && !isRight(table_join.kind))
                throw Exception("Illegal join kind for SEMI/ANTI join.", ErrorCodes::UNKNOWN_JOIN);
        }
        else if (isAsofJoin(table_join))
        {
            if (!(isInner(table_join.kind) || isLeft(table_join.kind)))
            {
                throw Exception("Join kind for Asof join must be either Inner or Left", ErrorCodes::UNKNOWN_JOIN);
            }
        }
    }

    ScopePtr joined_scope;

    if (table_join.using_expression_list)
        joined_scope = analyzeJoinUsing(table_join, left_scope, right_scope, right_table_qualifier, select_query);
    else
        joined_scope = analyzeJoinOn(table_join, left_scope, right_scope, right_table_qualifier);

    analysis.setScope(table_join, joined_scope);

    // validate join criteria for asof join
    {
        if (isAsofJoin(table_join))
        {
            if (table_join.using_expression_list)
            {
                if (analysis.join_using_results.at(&table_join).left_join_fields.size() < 2)
                    throw Exception("Join using list must have at least 2 elements for asof join", ErrorCodes::LOGICAL_ERROR);
            }
            else if (table_join.on_expression)
            {
                auto & join_on_result = analysis.join_on_results.at(&table_join);
                if (join_on_result.equality_conditions.empty())
                    throw Exception("Asof join must have at least 1 equality condition", ErrorCodes::LOGICAL_ERROR);
                if (join_on_result.inequality_conditions.size() != 1)
                    throw Exception("Asof join must have exact 1 inequality condition", ErrorCodes::LOGICAL_ERROR);
                if (!join_on_result.complex_expressions.empty())
                    throw Exception("Asof join must not have complex condition", ErrorCodes::LOGICAL_ERROR);
            }
            else
                throw Exception("Asof join must have join conditions", ErrorCodes::LOGICAL_ERROR);
        }
    }

    countLeadingHint(table_join);
    return joined_scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeJoinUsing(
    ASTTableJoin & table_join,
    ScopePtr left_scope,
    ScopePtr right_scope,
    const String & right_table_qualifier,
    ASTSelectQuery & select_query)
{
    auto & expr_list = table_join.using_expression_list->children;

    if (expr_list.empty())
        throw Exception("Empty join using list is not allowed.", ErrorCodes::LOGICAL_ERROR);

    std::vector<ASTPtr> join_key_asts;
    std::vector<size_t> left_join_fields;
    std::vector<size_t> right_join_fields;
    DataTypes left_coercions;
    DataTypes right_coercions;
    std::unordered_map<size_t, size_t> left_join_field_reverse_map;
    std::unordered_map<size_t, size_t> right_join_field_reverse_map;
    std::vector<bool> require_right_keys; // Clickhouse semantic specific
    NameSet seen_names;
    FieldDescriptions output_fields;

    bool make_nullable_for_left = isRightOrFull(table_join.kind) && context->getSettingsRef().join_use_nulls;
    bool make_nullable_for_right = isLeftOrFull(table_join.kind) && context->getSettingsRef().join_use_nulls;

    if (use_ansi_semantic)
    {
        auto resolve_join_key
            = [&](const QualifiedName & name, ScopePtr scope, bool left, std::vector<size_t> & join_field_indices) -> ResolvedField {
            std::optional<ResolvedField> resolved = scope->resolveFieldByAnsi(name);

            if (!resolved)
                throw Exception(
                    "Can not find column '" + name.toString() + "' in join " + (left ? "left" : "right") + " side",
                    ErrorCodes::UNKNOWN_IDENTIFIER);

            join_field_indices.emplace_back(resolved->hierarchy_index);
            analysis.addReadColumn(*resolved, true);
            return *resolved;
        };

        /// Step 1. resolve join key
        for (const auto & join_key_ast : expr_list)
        {
            auto * iden = join_key_ast->as<ASTIdentifier>();

            if (!iden)
                throw Exception("Expression except identifier is not allowed in join using list.", ErrorCodes::LOGICAL_ERROR);

            if (iden->compound())
                throw Exception("Compound identifier is not allowed in join using list.", ErrorCodes::LOGICAL_ERROR);

            if (!seen_names.insert(iden->name()).second)
                throw Exception("Duplicated join key found in join using list.", ErrorCodes::LOGICAL_ERROR);

            join_key_asts.push_back(join_key_ast);
            QualifiedName qualified_name{iden->name()};

            auto left_field = resolve_join_key(qualified_name, left_scope, true, left_join_fields);
            auto right_field = resolve_join_key(qualified_name, right_scope, false, right_join_fields);
            auto left_type = left_field.getFieldDescription().type;
            auto right_type = right_field.getFieldDescription().type;
            DataTypePtr output_type = nullptr;

            if (left_type->equals(*right_type))
            {
                output_type = left_type;
            }
            else if (enable_implicit_type_conversion)
            {
                try
                {
                    output_type = getCommonType(DataTypes{left_type, right_type}, enable_implicit_arg_type_convert, allow_extended_conversion);
                }
                catch (DB::Exception & ex)
                {
                    throw Exception(
                        "Type mismatch of columns to JOIN by: " + left_type->getName() + " at left, " + right_type->getName()
                            + " at right. " + "Can't get supertype: " + ex.message(),
                        ErrorCodes::TYPE_MISMATCH);
                }
            }

            if (!output_type)
                throw Exception("Type mismatch for join keys: " + serializeAST(table_join), ErrorCodes::TYPE_MISMATCH);

            left_coercions.push_back(left_type->equals(*output_type) ? nullptr : output_type);
            right_coercions.push_back(right_type->equals(*output_type) ? nullptr : output_type);
            output_fields.emplace_back(left_field.getFieldDescription().name, output_type);
        }

        /// Step 2. add non join fields
        auto add_non_join_fields = [&](ScopePtr scope, std::vector<size_t> & join_fields_list, bool make_nullable) {
            std::unordered_set<size_t> join_fields{join_fields_list.begin(), join_fields_list.end()};

            for (size_t i = 0; i < scope->size(); ++i)
            {
                if (join_fields.find(i) == join_fields.end())
                {
                    output_fields.push_back(scope->at(i));
                    if (make_nullable)
                        output_fields.back().type = JoinCommon::convertTypeToNullable(output_fields.back().type);
                }
            }
        };

        add_non_join_fields(left_scope, left_join_fields, make_nullable_for_left);
        add_non_join_fields(right_scope, right_join_fields, make_nullable_for_right);
    }
    else
    {
        DataTypes join_key_output_types;

        /// Step 1. resolve join key
        for (size_t i = 0, true_index = 0; i < expr_list.size(); ++i)
        {
            auto & join_key_ast = expr_list[i];
            String key_name = join_key_ast->getAliasOrColumnName();

            // see also 00702_join_with_using:
            // SELECT * FROM using1 ALL LEFT JOIN (SELECT * FROM using2) js2 USING (a, a, a, b, b, b, a, a) ORDER BY a;
            if (!seen_names.insert(key_name).second)
                continue;

            join_key_asts.push_back(join_key_ast);

            // Clickhouse allow join key is an expression. In this case, engine will calculate the expression
            // and use the result column as the join key. Note that this only apply for the left table.
            // e.g. 00057_join_aliases
            //     SELECT number, number / 2 AS n, j1, j2
            //     FROM system.numbers ANY LEFT JOIN
            //         (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10)
            //     USING n
            //     LIMIT 10
            // In rewrite phase, identifier 'n' will be rewritten to 'number / 2 AS n'.
            // For this case, we record join field as -1, and plan an additional projection before join.
            auto left_type = ExprAnalyzer::analyze(join_key_ast, left_scope, context, analysis, "JOIN USING"s);
            size_t left_field_index = -1;
            if (auto col_ref = analysis.tryGetColumnReference(join_key_ast))
            {
                left_field_index = col_ref->hierarchy_index;
            }
            left_join_fields.emplace_back(left_field_index);
            left_join_field_reverse_map[left_field_index] = true_index;


            // resolve name in right table
            auto resolved = right_scope->resolveFieldByClickhouse(key_name);

            if (!resolved)
                throw Exception("Can not find column " + key_name + " on right side", ErrorCodes::UNKNOWN_IDENTIFIER);

            right_join_fields.emplace_back(resolved->hierarchy_index);
            right_join_field_reverse_map[resolved->hierarchy_index] = true_index;
            analysis.addReadColumn(*resolved, true);

            auto right_type = resolved->getFieldDescription().type;
            DataTypePtr output_type = nullptr;

            if (left_type->equals(*right_type))
            {
                output_type = left_type;
            }
            else if (enable_implicit_type_conversion)
            {
                try
                {
                    output_type = getCommonType(DataTypes{left_type, right_type}, enable_implicit_arg_type_convert, allow_extended_conversion);
                }
                catch (DB::Exception & ex)
                {
                    throw Exception(
                        "Type mismatch of columns to JOIN by: " + left_type->getName() + " at left, " + right_type->getName()
                            + " at right. " + "Can't get supertype: " + ex.message(),
                        ErrorCodes::TYPE_MISMATCH);
                }
            }

            if (!output_type)
                throw Exception("Type mismatch for join keys: " + serializeAST(table_join), ErrorCodes::TYPE_MISMATCH);

            join_key_output_types.push_back(output_type);
            left_coercions.push_back(left_type->equals(*output_type) ? nullptr : output_type);
            right_coercions.push_back(right_type->equals(*output_type) ? nullptr : output_type);

            ++true_index;
        }

        /// Step 2. build output info
        // process left
        for (size_t i = 0; i < left_scope->size(); ++i)
        {
            const auto & input_field = left_scope->at(i);

            // TODO: one column used as multiple keys? e.g. SELECT a AS c, a AS d FROM (SELECT a, b FROM x) JOIN (SELECT c, d FROM y) USING (c, d)
            if (left_join_field_reverse_map.count(i))
            {
                output_fields.emplace_back(input_field.name, join_key_output_types[left_join_field_reverse_map[i]]);
            }
            else
            {
                output_fields.emplace_back(input_field);
                if (make_nullable_for_left)
                    output_fields.back().type = JoinCommon::convertTypeToNullable(output_fields.back().type);
            }
        }

        // process right
        auto select_query_ptr = select_query.ptr();
        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(select_query_ptr);

        auto source_columns = collectNames(left_scope);
        auto required_columns = columns_context.requiredColumns();
        require_right_keys.resize(right_join_fields.size(), false);
        bool check_identifier_begin_valid = context->getSettingsRef().check_identifier_begin_valid;

        for (size_t i = 0; i < right_scope->size(); ++i)
        {
            const auto & input_field = right_scope->at(i);
            auto new_name = qualifyJoinedName(input_field.name, right_table_qualifier, source_columns, check_identifier_begin_valid);

            if (!right_join_field_reverse_map.count(i))
            {
                output_fields.emplace_back(input_field.withNewName(new_name));
                if (make_nullable_for_right)
                    output_fields.back().type = JoinCommon::convertTypeToNullable(output_fields.back().type);
            }
            else if (required_columns.count(new_name) && !source_columns.count(new_name))
            {
                auto join_key_index = right_join_field_reverse_map[i];
                output_fields.emplace_back(new_name, join_key_output_types[join_key_index]);
                require_right_keys[join_key_index] = true;
            }
        }
    }

    analysis.join_using_results[&table_join] = JoinUsingAnalysis{
        join_key_asts,
        left_join_fields,
        left_coercions,
        right_join_fields,
        right_coercions,
        left_join_field_reverse_map,
        right_join_field_reverse_map,
        require_right_keys};
    return createScope(output_fields);
}

static constexpr int table_deps(int a, int b)
{
    return (a << 16) + b;
};

ScopePtr QueryAnalyzerVisitor::analyzeJoinOn(
    ASTTableJoin & table_join, ScopePtr left_scope, ScopePtr right_scope, const String & right_table_qualifier)
{
    ScopePtr output_scope;
    {
        FieldDescriptions output_fields;
        bool make_nullable_for_left = isRightOrFull(table_join.kind) && context->getSettingsRef().join_use_nulls;
        bool make_nullable_for_right = isLeftOrFull(table_join.kind) && context->getSettingsRef().join_use_nulls;
        auto update_type = [&](DataTypePtr & type, bool make_nullable)
        {
            if (make_nullable)
                return JoinCommon::convertTypeToNullable(type);
            return type;
        };

        if (use_ansi_semantic)
        {
            for (const auto & f : left_scope->getFields())
            {
                output_fields.emplace_back(f);
                output_fields.back().type = update_type(output_fields.back().type, make_nullable_for_left);
            }
            for (const auto & f : right_scope->getFields())
            {
                output_fields.emplace_back(f);
                output_fields.back().type = update_type(output_fields.back().type, make_nullable_for_right);
            }
        }
        else
        {
            for (const auto & f : left_scope->getFields())
            {
                output_fields.emplace_back(f);
                output_fields.back().type = update_type(output_fields.back().type, make_nullable_for_left);
            }

            auto source_names = collectNames(left_scope);
            bool check_identifier_begin_valid = context->getSettingsRef().check_identifier_begin_valid;

            for (const auto & f : right_scope->getFields())
            {
                auto new_name = qualifyJoinedName(f.name, right_table_qualifier, source_names, check_identifier_begin_valid);
                output_fields.emplace_back(f.withNewName(new_name));
                output_fields.back().type = update_type(output_fields.back().type, make_nullable_for_right);
            }
        }

        output_scope = createScope(output_fields);
    }

    if (table_join.on_expression)
    {
        // forbid arrayJoin in JOIN ON, see also the same check in CollectJoinOnKeysVisitor
        auto array_join_exprs = extractExpressions(context, analysis, table_join.on_expression, false, [&](const ASTPtr & node) {
            if (const auto * func = node->as<ASTFunction>())
                if (func->name == "arrayJoin" && (!context->getSettingsRef().ignore_array_join_check_in_join_on_condition))
                    return true;

            return false;
        });
        if (!array_join_exprs.empty())
            throw Exception(
                "Not allowed function in JOIN ON. Unexpected '" + queryToString(table_join.on_expression) + "'",
                ErrorCodes::INVALID_JOIN_ON_EXPRESSION);

        /// We split ON expression into CNF factors, and classify each factor into 3 categories:
        /// - Join Equality Condition: expressions with structure `expression_depends_left_table_fields = expression_depends_right_table_fields`
        /// - Join Inequality Condition: expressions with structure `expression_depends_left_table_fields >/>=/</<= expression_depends_right_table_fields`
        /// - Complex Expression: expressions not belong to above 2 categories
        std::vector<JoinEqualityCondition> equality_conditions;
        std::vector<JoinInequalityCondition> inequality_conditions;
        std::vector<ASTPtr> complex_expressions;

        /// return  -1 - no dependencies
        ///          0 - dependencies come from both left side & right side
        ///          1 - dependencies only come from left side
        ///          2 - dependencies only come from right side
        auto choose_table_for_dependencies = [&](const std::vector<ASTPtr> & dependencies) -> int {
            int table = -1;

            for (const auto & dep : dependencies)
            {
                auto & resolved = analysis.column_references.at(dep);
                int table_for_this_dep = resolved.local_index < left_scope->size() ? 1 : 2;

                if (table < 0)
                    table = table_for_this_dep;
                else if (table != table_for_this_dep)
                    return 0;
            }

            return table;
        };

        for (auto & conjunct : expressionToCnf(table_join.on_expression))
        {
            ExprAnalyzer::analyze(conjunct, output_scope, context, analysis, "JOIN ON expression"s);

            bool is_join_expr = false;

            if (auto * func = conjunct->as<ASTFunction>(); func && isComparisonFunction(*func))
            {
                auto & left_arg = func->arguments->children[0];
                auto & right_arg = func->arguments->children[1];

                // extract column references in arguments, but exclude column references from outer scope
                auto left_dependencies = extractReferencesToScope(context, analysis, left_arg, output_scope);
                auto right_dependencies = extractReferencesToScope(context, analysis, right_arg, output_scope);

                size_t table_for_left = choose_table_for_dependencies(left_dependencies);
                size_t table_for_right = choose_table_for_dependencies(right_dependencies);

                using namespace ASOF;

                auto add_join_exprs = [&](const ASTPtr & left_ast, const ASTPtr & right_ast, Inequality inequality) {
                    DataTypePtr left_coercion = nullptr;
                    DataTypePtr right_coercion = nullptr;

                    // for non-ASOF join, inequality_conditions will be included in join filter, so don't have to do type coercion
                    if ((func->name == "equals" || func->name == "bitEquals") || isAsofJoin(table_join))
                    {
                        DataTypePtr left_type = analysis.getExpressionType(left_ast);
                        DataTypePtr right_type = analysis.getExpressionType(right_ast);

                        if (!JoinCommon::isJoinCompatibleTypes(left_type, right_type))
                        {
                            DataTypePtr super_type = nullptr;

                            if (enable_implicit_type_conversion)
                            {
                                try
                                {
                                    super_type = getCommonType(DataTypes{left_type, right_type}, enable_implicit_arg_type_convert, allow_extended_conversion);
                                }
                                catch (DB::Exception & ex)
                                {
                                    throw Exception(
                                        "Type mismatch of columns to JOIN by: " + left_type->getName() + " at left, "
                                            + right_type->getName() + " at right. " + "Can't get supertype: " + ex.message(),
                                        ErrorCodes::TYPE_MISMATCH);
                                }
                                if (!left_type->equals(*super_type))
                                    left_coercion = super_type;
                                if (!right_type->equals(*super_type))
                                    right_coercion = super_type;
                            }

                            if (!super_type)
                            {
                                throw Exception("Type mismatch for join keys: " + serializeAST(table_join), ErrorCodes::TYPE_MISMATCH);
                            }
                        }
                    }

                    if (func->name == "equals" || func->name == "bitEquals")
                        equality_conditions.emplace_back(left_ast, right_ast, left_coercion, right_coercion, func->name == "bitEquals");
                    else
                        inequality_conditions.emplace_back(left_ast, right_ast, inequality, left_coercion, right_coercion);

                    is_join_expr = true;
                };

                switch (table_deps(table_for_left, table_for_right))
                {
                    case table_deps(1, 2):
                    case table_deps(1, -1):
                    case table_deps(-1, 2):
                    case table_deps(-1, -1):
                        add_join_exprs(left_arg, right_arg, getInequality(func->name));
                        break;
                    case table_deps(2, 1):
                    case table_deps(2, -1):
                    case table_deps(-1, 1):
                        add_join_exprs(right_arg, left_arg, reverseInequality(getInequality(func->name)));
                        break;
                }
            }

            if (!is_join_expr)
                complex_expressions.push_back(conjunct);
        }

        analysis.join_on_results[&table_join] = JoinOnAnalysis{equality_conditions, inequality_conditions, complex_expressions};
    }

    return output_scope;
}

ScopePtr QueryAnalyzerVisitor::analyzeArrayJoin(ASTArrayJoin & array_join, ASTSelectQuery & select_query, ScopePtr source_scope)
{
    ASTPtr array_join_expression_list = array_join.expression_list;
    if (array_join_expression_list->children.empty())
        throw DB::Exception("ARRAY JOIN requires an argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ExprAnalyzerOptions expr_options{"Array Join expression"};
    expr_options.selectQuery(select_query)
        .subquerySupport(ExprAnalyzerOptions::SubquerySupport::DISALLOWED)
        .aggregateSupport(ExprAnalyzerOptions::AggregateSupport::DISALLOWED)
        .windowSupport(ExprAnalyzerOptions::WindowSupport::DISALLOWED);

    ArrayJoinDescriptions array_join_descs;
    FieldDescriptions output_fields = source_scope->getFields();
    NameSet name_set;
    for (auto & array_join_expr : array_join_expression_list->children)
    {
        String output_name = array_join_expr->getAliasOrColumnName();
        if (name_set.count(output_name))
            throw Exception("Duplicate alias in ARRAY JOIN: " + output_name, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

        // there are 3 array join scenarios:
        //   1. join an array column, e.g. ARRAY JOIN arr AS elem
        //   2. join an array expression, e.g. ARRAY JOIN [1, 2, 3] AS elem
        //   3. join a nested table, this means join all columns of this nested table, e.g. SELECT flatten.foo ARRAY JOIN nested AS flatten
        if (array_join_expr->as<ASTIdentifier>())
        {
            // handle 1 & 3
            String column_name = array_join_expr->getColumnName();
            bool create_new_field = output_name != column_name;
            bool matched = false;

            for (size_t field_index = 0; field_index < source_scope->size(); ++field_index)
            {
                const auto & field = source_scope->at(field_index);
                if (!field.can_be_array_joined)
                    continue;

                auto split = Nested::splitName(field.name);
                String actual_output_name;
                if (column_name == field.name)
                    actual_output_name = output_name;
                else if (column_name == split.first)
                    actual_output_name = Nested::concatenateName(output_name, split.second);
                else
                    continue;

                matched = true;

                const auto array_type = getArrayJoinDataType(field.type);
                if (!array_type)
                    throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);

                analysis.addReadColumn(ResolvedField{source_scope, field_index}, true);
                array_join_descs.emplace_back(ArrayJoinDescription{field_index, create_new_field});
                if (create_new_field)
                    output_fields.emplace_back(actual_output_name, array_type->getNestedType());
                else
                    output_fields[field_index] = FieldDescription{field.name, array_type->getNestedType()};
            }

            if (!matched)
                throw Exception("Can not resolve array join column: " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);
        }
        else
        {
            if (array_join_expr->tryGetAlias().empty())
                throw Exception(
                    "No alias for non-trivial value in ARRAY JOIN: " + array_join_expr->tryGetAlias(), ErrorCodes::ALIAS_REQUIRED);

            auto array_join_expr_type = ExprAnalyzer::analyze(array_join_expr, source_scope, context, analysis, expr_options);
            const auto array_type = getArrayJoinDataType(array_join_expr_type);
            if (!array_type)
                throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);

            array_join_descs.emplace_back(ArrayJoinDescription{array_join_expr, true});
            output_fields.emplace_back(output_name, array_type->getNestedType());
        }
    }

    analysis.array_join_analysis[&select_query] = ArrayJoinAnalysis{(array_join.kind == ASTArrayJoin::Kind::Left), array_join_descs};
    ScopePtr output_scope = createScope(output_fields);
    analysis.setScope(array_join, output_scope);
    return output_scope;
}

void QueryAnalyzerVisitor::analyzeWindow(ASTSelectQuery & select_query)
{
    if (!select_query.window())
        return;

    auto & window_list = select_query.window()->children;

    for (auto & window : window_list)
    {
        auto & window_elem = window->as<ASTWindowListElement &>();

        const auto & registered_windows = analysis.getRegisteredWindows(select_query);

        if (registered_windows.find(window_elem.name) != registered_windows.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window '{}' is defined twice in the WINDOW clause", window_elem.name);

        auto resolved_window = resolveWindow(window_elem.definition, registered_windows, context);
        analysis.setRegisteredWindow(select_query, window_elem.name, resolved_window);
    }
}

void QueryAnalyzerVisitor::analyzePrewhere(
    ASTSelectQuery & select_query, ScopePtr source_scope, ASTPtr & alias_columns, const Aliases & query_aliases)
{
    if (!select_query.prewhere())
        return;

    if (select_query.join())
        throw Exception("Prewhere is not supported in query with join", ErrorCodes::ILLEGAL_PREWHERE);

    auto prewhere = select_query.prewhere()->clone();
    // normalize query aliases in ANSI mode
    if (use_ansi_semantic)
        normalizeAliases(prewhere, query_aliases, source_scope->getNamesSet());
    // normalize ALIAS columns
    normalizeAliases(prewhere, alias_columns);

    ExprAnalyzerOptions expr_options{"PREWHERE expression"};
    expr_options.selectQuery(select_query).subquerySupport(ExprAnalyzerOptions::SubquerySupport::DISALLOWED);
    auto prewhere_filter_type = ExprAnalyzer::analyze(prewhere, source_scope, context, analysis, expr_options);
    if (auto inner_type = removeNullable(removeLowCardinality(prewhere_filter_type)))
    {
        if (!inner_type->equals(DataTypeUInt8()) && !inner_type->equals(DataTypeNothing()))
            throw Exception(
                "Illegal type " + prewhere_filter_type->getName() + " for PREWHERE. Must be UInt8 or Nullable(UInt8).",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
    }
    analysis.setPrewhere(select_query, prewhere);
}

void QueryAnalyzerVisitor::analyzeWhere(ASTSelectQuery & select_query, ScopePtr source_scope)
{
    if (!select_query.where())
        return;

    ExprAnalyzerOptions expr_options{"WHERE expression"};
    expr_options.selectQuery(select_query).subquerySupport(ExprAnalyzerOptions::SubquerySupport::CORRELATED).subqueryToSemiAnti(true);
    auto filter_type = ExprAnalyzer::analyze(select_query.refWhere(), source_scope, context, analysis, expr_options);

    if (auto inner_type = removeNullable(removeLowCardinality(filter_type)))
    {
        if (!inner_type->equals(DataTypeUInt8()) && !inner_type->equals(DataTypeNothing()))
            throw Exception(
                "Illegal type " + filter_type->getName() + " for WHERE. Must be UInt8 or Nullable(UInt8).",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
    }
}

ASTs QueryAnalyzerVisitor::analyzeSelect(ASTSelectQuery & select_query, ScopePtr source_scope)
{
    if (!select_query.select() || select_query.refSelect()->children.empty())
        throw Exception("Empty select list found", ErrorCodes::LOGICAL_ERROR);

    ASTs select_expressions;
    FieldDescriptions output_description;
    ExprAnalyzerOptions expr_options{"SELECT expression"};

    expr_options.selectQuery(select_query)
        .subquerySupport(ExprAnalyzerOptions::SubquerySupport::CORRELATED)
        .aggregateSupport(ExprAnalyzerOptions::AggregateSupport::ALLOWED)
        .windowSupport(ExprAnalyzerOptions::WindowSupport::ALLOWED);

    auto add_select_expression = [&](ASTPtr & expression) {
        auto expression_type = ExprAnalyzer::analyze(expression, source_scope, context, analysis, expr_options);

        auto get_output_name = [&](const ASTPtr & expr) -> String {
            // clickhouse semantic
            if (!use_ansi_semantic)
            {
                if (expr->as<ASTFieldReference>())
                {
                    auto col_ref = analysis.tryGetColumnReference(expr);
                    if (!col_ref)
                        throw Exception("invalid select expression", ErrorCodes::LOGICAL_ERROR);

                    return col_ref->getFieldDescription().name;
                }
                else
                    return expr->getAliasOrColumnName();
            }

            // AnsiSql semantic
            if (!expr->tryGetAlias().empty())
                return expr->tryGetAlias();
            else if (auto field_ref = analysis.tryGetColumnReference(expr))
                return field_ref->getFieldDescription().name;
            else
                return expr->getAliasOrColumnName();
        };

        auto output_name = get_output_name(expression);
        FieldDescription output_field{output_name, expression_type};

        if (auto source_field = analysis.tryGetColumnReference(expression))
        {
            output_field.copyOriginInfo(source_field->getFieldDescription());
        }

        select_expressions.push_back(expression);
        output_description.push_back(output_field);
    };

    for (auto & select_item : select_query.refSelect()->children)
    {
        if (select_item->as<ASTAsterisk>())
        {
            for (size_t field_index = 0; field_index < source_scope->size(); ++field_index)
            {
                if (source_scope->at(field_index).substituted_by_asterisk)
                {
                    ASTPtr field_reference = std::make_shared<ASTFieldReference>(field_index);
                    add_select_expression(field_reference);
                }
            }
        }
        else if (select_item->as<ASTQualifiedAsterisk>())
        {
            if (select_item->children.empty() || !select_item->getChildren()[0]->as<ASTTableIdentifier>())
                throw Exception("Unable to resolve qualified asterisk", ErrorCodes::UNKNOWN_IDENTIFIER);

            ASTIdentifier & astidentifier = select_item->getChildren()[0]->as<ASTTableIdentifier &>();
            auto prefix = QualifiedName::extractQualifiedName(astidentifier);
            bool matched = false;

            for (size_t field_index = 0; field_index < source_scope->size(); ++field_index)
            {
                if (source_scope->at(field_index).substituted_by_asterisk && source_scope->at(field_index).prefix.hasSuffix(prefix))
                {
                    matched = true;
                    ASTPtr field_reference = std::make_shared<ASTFieldReference>(field_index);
                    add_select_expression(field_reference);
                }
            }
            if (!matched)
                throw Exception("Can not find column of " + prefix.toString() + " in Scope", ErrorCodes::UNKNOWN_IDENTIFIER);
        }
        else if (auto * asterisk_pattern = select_item->as<ASTColumnsMatcher>())
        {
            for (size_t field_index = 0; field_index < source_scope->size(); ++field_index)
            {
                if (source_scope->at(field_index).substituted_by_asterisk
                    && asterisk_pattern->isColumnMatching(source_scope->at(field_index).name))
                {
                    ASTPtr field_reference = std::make_shared<ASTFieldReference>(field_index);
                    add_select_expression(field_reference);
                }
            }
        }
        else if (select_item->as<ASTFunction>() && select_item->as<ASTFunction>()->name == "untuple")
        {
            auto * function = select_item->as<ASTFunction>();
            if (function->arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function untuple doesn't match. Passed {}, should be 1",
                    function->arguments->children.size());

            auto expression_type = ExprAnalyzer::analyze(function->arguments->children[0], source_scope, context, analysis, expr_options);
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(expression_type.get());
            if (!tuple_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function untuple expect tuple argument, got {}", expression_type->getName());

            size_t tid = 0;
            for (const auto & name [[maybe_unused]] : tuple_type->getElementNames())
            {
                auto tuple_ast = function->arguments->children[0];
                auto literal = std::make_shared<ASTLiteral>(UInt64(++tid));
                ASTPtr func = makeASTFunction("tupleElement", tuple_ast, literal);
                add_select_expression(func);
            }
        }
        else
            add_select_expression(select_item);
    }

    analysis.select_expressions[&select_query] = select_expressions;
    analysis.setOutputDescription(select_query, output_description);
    return select_expressions;
}

void QueryAnalyzerVisitor::analyzeGroupBy(ASTSelectQuery & select_query, ASTs & select_expressions, ScopePtr source_scope)
{
    std::vector<ASTPtr> grouping_expressions;
    std::vector<std::vector<ASTPtr>> grouping_sets;

    if (select_query.groupBy())
    {
        bool allow_group_by_position = context->getSettingsRef().enable_positional_arguments && !select_query.group_by_with_rollup
            && !select_query.group_by_with_cube && !select_query.group_by_with_grouping_sets;

        auto analyze_grouping_set = [&](ASTs & grouping_expr_list) {
            std::vector<ASTPtr> analyzed_grouping_set;

            for (ASTPtr grouping_expr : grouping_expr_list)
            {
                if (allow_group_by_position)
                    if (auto * literal = grouping_expr->as<ASTLiteral>(); literal && literal->tryGetAlias().empty()
                        && // avoid aliased expr being interpreted as positional argument
                        // e.g. SELECT 1 AS a ORDER BY a
                        literal->value.getType() == Field::Types::UInt64)
                    {
                        auto index = literal->value.get<UInt64>();
                        if (index > select_expressions.size() || index < 1)
                        {
                            throw Exception("Group by index is greater than the number of select elements", ErrorCodes::BAD_ARGUMENTS);
                        }
                        grouping_expr = select_expressions[index - 1];
                    }

                // if grouping expr has been analyzed(i.e. it is from select expression), only check there is no agg/window/grouping
                if (analysis.hasExpressionColumnWithType(grouping_expr))
                {
                    verifyNoAggregateWindowOrGroupingOperations(grouping_expr, "GROUP BY expression");
                }
                else
                {
                    // TODO @wangtao
                    // Current we don't distinct CORRELATED or UNCORRELATED subquery in group by.
                    // CORRELATED/UNCORRELATED means we support subquery in group by.
                    // Should check CORRELATED subquery later.
                    ExprAnalyzerOptions expr_options{"GROUP BY expression"};
                    expr_options.selectQuery(select_query).subquerySupport(ExprAnalyzerOptions::SubquerySupport::CORRELATED);
                    // ExprAnalyzer::analyze(grouping_expr, source_scope, context, analysis, "GROUP BY expression"s);
                    ExprAnalyzer::analyze(grouping_expr, source_scope, context, analysis, expr_options);
                }

                analyzed_grouping_set.push_back(grouping_expr);
            }

            grouping_sets.push_back(analyzed_grouping_set);
            grouping_expressions.insert(grouping_expressions.end(), analyzed_grouping_set.begin(), analyzed_grouping_set.end());
        };

        if (select_query.group_by_with_grouping_sets)
        {
            for (auto & grouping_set_element : select_query.groupBy()->children)
                analyze_grouping_set(grouping_set_element->children);
        }
        else
        {
            analyze_grouping_set(select_query.groupBy()->children);
        }

        if (select_query.group_by_with_totals)
        {
            if (select_query.group_by_with_cube || select_query.group_by_with_rollup || select_query.group_by_with_grouping_sets)
            {
                throw Exception("WITH TOTALS and ROLLUP/CUBE/GROUPING SETS are not supported together", ErrorCodes::NOT_IMPLEMENTED);
            }
        }
    }

    analysis.group_by_results[&select_query] = GroupByAnalysis{std::move(grouping_expressions), std::move(grouping_sets)};
}

/*
void QueryAnalyzerVisitor::analyzeInterestEvents(ASTSelectQuery & select_query)
{
    if (select_query.interestEvents())
    {
        std::vector<std::pair<String, UInt16>> interestevents_info_list;

        auto explicitAST = typeid_cast<const ASTInterestEvents *>(select_query.interestEvents().get());
        if (!explicitAST)
            return;

        auto event_list = typeid_cast<const ASTExpressionList *>(explicitAST->events.get());
        if (!event_list)
        {
            throw Exception(
                "INTEREST EVENTS should be list of string literal, but it is: " + serializeAST(*select_query.interestEvents()),
                ErrorCodes::BAD_ARGUMENTS);
        }

        UInt16 numEvents = interestevents_info_list.size();
        for (auto & child : event_list->children)
        {
            const ASTLiteral * lit = typeid_cast<ASTLiteral *>(child.get());
            if (!lit)
            {
                throw Exception(
                    "INTEREST EVENTS should be list of string literal, but it is: " + serializeAST(*select_query.interestEvents()),
                    ErrorCodes::BAD_ARGUMENTS);
            }

            String event = lit->value.safeGet<String>();

            interestevents_info_list.emplace_back(event, numEvents++);
        }

        if (interestevents_info_list.empty())
        {
            interestevents_info_list.emplace_back("empty", 1);
        }

        analysis.interest_events[&select_query] = std::move(interestevents_info_list);
    }
}
*/

void QueryAnalyzerVisitor::analyzeHaving(ASTSelectQuery & select_query, ScopePtr source_scope)
{
    if (!select_query.having())
        return;

    ExprAnalyzerOptions expr_options{"HAVING clause"};
    expr_options.selectQuery(select_query)
        .subquerySupport(ExprAnalyzerOptions::SubquerySupport::CORRELATED)
        .aggregateSupport(ExprAnalyzerOptions::AggregateSupport::ALLOWED);
    ExprAnalyzer::analyze(select_query.refHaving(), source_scope, context, analysis, expr_options);
}

void QueryAnalyzerVisitor::analyzeOrderBy(ASTSelectQuery & select_query, ASTs & select_expressions, ScopePtr output_scope)
{
    if (select_query.orderBy())
    {
        std::vector<std::shared_ptr<ASTOrderByElement>> result;

        ExprAnalyzerOptions expr_options{"ORDER BY expression"};

        expr_options.selectQuery(select_query)
            .subquerySupport(ExprAnalyzerOptions::SubquerySupport::CORRELATED)
            .aggregateSupport(ExprAnalyzerOptions::AggregateSupport::ALLOWED)
            .windowSupport(ExprAnalyzerOptions::WindowSupport::ALLOWED);

        if (context->getSettingsRef().enable_order_by_all && select_query.order_by_all)  
            expandOrderByAll(&select_query);
        for (auto order_item : select_query.orderBy()->children)
        {
            auto & order_elem = order_item->as<ASTOrderByElement &>();

            if (context->getSettingsRef().enable_positional_arguments)
                if (auto * literal = order_elem.children.front()->as<ASTLiteral>(); literal && literal->tryGetAlias().empty()
                    && // avoid aliased expr being interpreted as positional argument
                    // e.g. SELECT 1 AS a ORDER BY a
                    literal->value.getType() == Field::Types::UInt64)
                {
                    auto index = literal->value.get<UInt64>();
                    if (index > select_expressions.size() || index < 1)
                    {
                        throw Exception("Order by index is greater than the number of select elements", ErrorCodes::BAD_ARGUMENTS);
                    }

                    order_item = order_item->clone();
                    order_item->children.clear();
                    // TODO: use ASTFieldReference to avoid unnecessary projection
                    order_item->children.push_back(select_expressions[index - 1]);
                }

            if (!analysis.hasExpressionColumnWithType(order_item->children.front()))
                ExprAnalyzer::analyze(order_item, output_scope, context, analysis, expr_options);

            result.push_back(std::dynamic_pointer_cast<ASTOrderByElement>(order_item));
        }

        analysis.order_by_results[&select_query] = result;
    }
}

void QueryAnalyzerVisitor::analyzeLimitBy(ASTSelectQuery & select_query, ASTs & select_expressions, ScopePtr output_scope)
{
    if (select_query.limitBy())
    {
        ExprAnalyzerOptions expr_options{"LIMIT BY expression"};

        expr_options.selectQuery(select_query)
            .subquerySupport(ExprAnalyzerOptions::SubquerySupport::CORRELATED)
            .aggregateSupport(ExprAnalyzerOptions::AggregateSupport::ALLOWED)
            .windowSupport(ExprAnalyzerOptions::WindowSupport::ALLOWED);

        for (auto limit_item : select_query.limitBy()->children)
        {
            if (context->getSettingsRef().enable_positional_arguments)
                if (auto * literal = limit_item->as<ASTLiteral>(); literal && literal->tryGetAlias().empty()
                    && // avoid aliased expr being interpreted as positional argument
                    // e.g. SELECT 1 AS a ORDER BY a
                    literal->value.getType() == Field::Types::UInt64)
                {
                    auto index = literal->value.get<UInt64>();
                    if (index > select_expressions.size() || index < 1)
                    {
                        throw Exception("Limit by index is greater than the number of select elements", ErrorCodes::BAD_ARGUMENTS);
                    }
                    limit_item = select_expressions[index - 1];
                }

            ExprAnalyzer::analyze(limit_item, output_scope, context, analysis, expr_options);
            analysis.limit_by_items[&select_query].push_back(limit_item);
        }

        auto limit_by_value = analyzeUIntConstExpression(select_query.limitByLength());
        analysis.limit_by_values[&select_query] = limit_by_value;

        if (select_query.getLimitByOffset())
        {
            auto limit_by_offset_value = analyzeUIntConstExpression(select_query.getLimitByOffset());
            analysis.limit_by_offset_values[&select_query] = limit_by_offset_value;
        }
    }
}

void QueryAnalyzerVisitor::analyzeLimitAndOffset(ASTSelectQuery & select_query)
{
    if (select_query.limitLength())
        analysis.limit_lengths[&select_query] = analyzeUIntConstExpression(select_query.limitLength());

    if (select_query.limitOffset())
        analysis.limit_offsets[&select_query] = analyzeUIntConstExpression(select_query.limitOffset());
}

ScopePtr QueryAnalyzerVisitor::createScope(FieldDescriptions field_descriptions, ScopePtr parent)
{
    bool query_boundary = false;

    if (!parent)
    {
        parent = outer_query_scope;
        query_boundary = true;
    }

    return analysis.scope_factory.createScope(Scope::ScopeType::RELATION, parent, query_boundary, std::move(field_descriptions));
}

DatabaseAndTableWithAlias QueryAnalyzerVisitor::extractTableWithAlias(const ASTTableExpression & table_expression)
{
    return DatabaseAndTableWithAlias{table_expression, context->getCurrentDatabase()};
}

void QueryAnalyzerVisitor::analyzeOutfile(ASTSelectWithUnionQuery & outfile_query)
{
    String out_file;
    String format;
    String compression_method;
    size_t compression_level = 0;

    if (outfile_query.out_file)
    {
        out_file = outfile_query.out_file->as<ASTLiteral &>().value.safeGet<std::string>();

        if (outfile_query.format)
        {
            format = outfile_query.format->as<ASTIdentifier &>().name();
        }

        if (outfile_query.compression_method)
        {
            compression_method = outfile_query.compression_method->as<ASTLiteral &>().value.safeGet<std::string>();

            if (outfile_query.compression_level)
            {
                const auto & compression_level_node = outfile_query.compression_level->as<ASTLiteral &>();
                if (!compression_level_node.value.tryGet<UInt64>(compression_level))
                {
                    throw Exception("Invalid compression level", ErrorCodes::BAD_ARGUMENTS);
                }
            }
        }
        analysis.outfile_analysis = OutfileAnalysis{out_file, format, compression_method, compression_level};
    }
}

namespace
{
    class VerifyNoAggregateWindowOrGroupingOperationsVisitor : public AnalyzerExpressionVisitor<const Void>
    {
    public:
        VerifyNoAggregateWindowOrGroupingOperationsVisitor(String statement_name_, ContextPtr context_)
            : AnalyzerExpressionVisitor(context_), statement_name(std::move(statement_name_))
        {
        }

    protected:
        void visitExpression(ASTPtr &, IAST &, const Void &) override { }

        void visitAggregateFunction(ASTPtr &, ASTFunction &, const Void &) override
        {
            throwException("Aggregate function", ErrorCodes::ILLEGAL_AGGREGATION);
        }

        void visitWindowFunction(ASTPtr &, ASTFunction &, const Void &) override
        {
            throwException("Window function", ErrorCodes::ILLEGAL_AGGREGATION);
        }

        void visitGroupingOperation(ASTPtr &, ASTFunction &, const Void &) override
        {
            throwException("Grouping operation", ErrorCodes::ILLEGAL_AGGREGATION);
        }

    private:
        String statement_name;

        [[noreturn]] void throwException(const String & expr_type, int code)
        {
            throw Exception(expr_type + " is not allowed in " + statement_name, code);
        }
    };
}

void QueryAnalyzerVisitor::verifyNoAggregateWindowOrGroupingOperations(ASTPtr & expression, const String & statement_name)
{
    VerifyNoAggregateWindowOrGroupingOperationsVisitor visitor{statement_name, context};
    traverseExpressionTree(expression, visitor, {}, analysis, context);
}

namespace
{
    class PostAggregateAnalyzerExpressionVisitor : public AnalyzerExpressionVisitor<const Void>
    {
    protected:
        void visitExpression(ASTPtr &, IAST &, const Void &) override { }

        void visitIdentifier(ASTPtr & node, ASTIdentifier &, const Void &) override { verifyNoReferenceToNonGroupingKey(node); }

        void visitFieldReference(ASTPtr & node, ASTFieldReference &, const Void &) override { verifyNoReferenceToNonGroupingKey(node); }

        void visitScalarSubquery(ASTPtr & node, ASTSubquery &, const Void &) override { verifyNoReferenceToNonGroupingKeyInSubquery(node); }

        void visitInSubquery(ASTPtr &, ASTFunction & function, const Void &) override
        {
            verifyNoReferenceToNonGroupingKeyInSubquery(function.arguments->children[1]);
        }

        void visitQuantifiedComparisonSubquery(ASTPtr &, ASTQuantifiedComparison & ast, const Void &) override
        {
            verifyNoReferenceToNonGroupingKeyInSubquery(ast.children[1]);
        }

        void visitExistsSubquery(ASTPtr & node, ASTFunction &, const Void &) override { verifyNoReferenceToNonGroupingKeyInSubquery(node); }

    private:
        Analysis & analysis;
        ScopePtr source_scope;
        std::unordered_set<size_t> grouping_field_indices;

        void verifyNoReferenceToNonGroupingKey(ASTPtr & node);
        void verifyNoReferenceToNonGroupingKeyInSubquery(ASTPtr & node);

    public:
        PostAggregateAnalyzerExpressionVisitor(
            ContextPtr context_, Analysis & analysis_, ScopePtr source_scope_, std::unordered_set<size_t> grouping_field_indices_)
            : AnalyzerExpressionVisitor(context_)
            , analysis(analysis_)
            , source_scope(source_scope_)
            , grouping_field_indices(std::move(grouping_field_indices_))
        {
        }
    };

    void PostAggregateAnalyzerExpressionVisitor::verifyNoReferenceToNonGroupingKey(ASTPtr & node)
    {
        // cases when col_ref->scope != source_scope:
        // 1. outer query scope
        // 2. order by scope
        if (auto col_ref = analysis.tryGetColumnReference(node); col_ref && col_ref->scope == source_scope)
        {
            if (!grouping_field_indices.count(col_ref->local_index))
                throw Exception(serializeAST(*node) + " is not a grouping key", ErrorCodes::NOT_AN_AGGREGATE);
        }
    }

    void PostAggregateAnalyzerExpressionVisitor::verifyNoReferenceToNonGroupingKeyInSubquery(ASTPtr & node)
    {
        auto col_ref_exprs = extractReferencesToScope(context, analysis, node, source_scope, true);
        for (const auto & col_ref_expr : col_ref_exprs)
        {
            if (auto col_ref = analysis.tryGetColumnReference(col_ref_expr))
                if (!grouping_field_indices.count(col_ref->local_index))
                    throw Exception("Non grouping key used in subquery: " + serializeAST(*node), ErrorCodes::NOT_AN_AGGREGATE);
        }
    }

    class PostAggregateExpressionTraverser : public ExpressionTraversalVisitor<const Void>
    {
    protected:
        // don't go into aggregate function, since it will be evaluated during aggregate phase
        void visitAggregateFunction(ASTPtr &, ASTFunction &, const Void &) override { }

    public:
        void process(ASTPtr & node, const Void & traversal_context) override
        {
            // don't go into grouping expression
            if (grouping_expressions.count(node))
                return;

            ExpressionTraversalVisitor::process(node, traversal_context);
        }

        PostAggregateExpressionTraverser(
            AnalyzerExpressionVisitor<const Void> & user_visitor_,
            const Void & user_context_,
            Analysis & analysis_,
            ContextPtr context_,
            ScopeAwaredASTSet grouping_expressions_)
            : ExpressionTraversalVisitor(user_visitor_, user_context_, analysis_, context_)
            , grouping_expressions(std::move(grouping_expressions_))
        {
        }

        using ExpressionTraversalVisitor::process;

    private:
        ScopeAwaredASTSet grouping_expressions;
    };

}

void QueryAnalyzerVisitor::verifyAggregate(ASTSelectQuery & select_query, ScopePtr source_scope)
{
    if (!analysis.needAggregate(select_query))
    {
        // verify no grouping operation exists if query won't be aggregated
        if (!analysis.getGroupingOperations(select_query).empty())
        {
            auto & representative = analysis.getGroupingOperations(select_query)[0];
            throw Exception("Invalid grouping operation: " + serializeAST(*representative), ErrorCodes::BAD_ARGUMENTS);
        }

        return;
    }

    ScopeAwaredASTSet grouping_expressions = createScopeAwaredASTSet(analysis, source_scope);
    std::unordered_set<size_t> grouping_field_indices;
    auto & group_by_analysis = analysis.getGroupByAnalysis(select_query);

    for (const auto & group_by_key : group_by_analysis.grouping_expressions)
    {
        grouping_expressions.emplace(group_by_key);
        if (auto col_ref = analysis.tryGetColumnReference(group_by_key); col_ref && col_ref->scope == source_scope)
            grouping_field_indices.emplace(col_ref->local_index);
    }

    // verify argument of grouping operations are grouping keys
    if (auto & grouping_ops = analysis.getGroupingOperations(select_query); !grouping_ops.empty())
    {
        for (const auto & grouping_op : grouping_ops)
            for (const auto & grouping_op_arg : grouping_op->arguments->children)
                if (!grouping_expressions.count(grouping_op_arg))
                    throw Exception("Invalid grouping operation: " + serializeAST(*grouping_op), ErrorCodes::BAD_ARGUMENTS);
    }

    // verify no reference to non grouping fields after aggregate
    if (!context->getSettingsRef().only_full_group_by)
        return;

    // verify no reference to non grouping fields after aggregate
    PostAggregateAnalyzerExpressionVisitor post_agg_visitor{context, analysis, source_scope, grouping_field_indices};
    PostAggregateExpressionTraverser post_agg_traverser{post_agg_visitor, {}, analysis, context, grouping_expressions};

    ASTs source_expressions;

    if (select_query.having())
        source_expressions.push_back(select_query.having());

    for (const auto & expr : analysis.getSelectExpressions(select_query))
        source_expressions.push_back(expr);

    for (const auto & expr : analysis.getOrderByAnalysis(select_query))
        source_expressions.push_back(expr);

    if (select_query.limitBy())
        source_expressions.push_back(select_query.limitBy());

    for (auto & expr : source_expressions)
        post_agg_traverser.process(expr);
}

namespace
{

    class FreeReferencesToLambdaArgumentVisitor : public AnalyzerExpressionVisitor<const Void>
    {
    public:
        FreeReferencesToLambdaArgumentVisitor(String function_name_, int error_code_, ContextPtr context_, Analysis & analysis_)
            : AnalyzerExpressionVisitor(context_), analysis(analysis_), function_name(std::move(function_name_)), error_code(error_code_)
        {
        }

    protected:
        void visitExpression(ASTPtr &, IAST &, const Void &) override { }

        void visitLambdaExpression(ASTPtr &, ASTFunction & ast, const Void &) override { lambda_scopes.emplace(analysis.getScope(ast)); }

        void visitIdentifier(ASTPtr & node, ASTIdentifier &, const Void &) override
        {
            if (auto lambda_ref = analysis.tryGetLambdaArgumentReference(node); lambda_ref && !lambda_scopes.count(lambda_ref->scope))
                throw Exception("Free lambda argument reference found in " + function_name + ": " + serializeAST(*node), error_code);
        }

        // no need to override visitFieldReference, as it can not refer to lambda argument
    private:
        Analysis & analysis;
        String function_name;
        int error_code;
        std::unordered_set<ScopePtr> lambda_scopes;
    };
}

void QueryAnalyzerVisitor::verifyNoFreeReferencesToLambdaArgument(ASTSelectQuery & select_query)
{
    FreeReferencesToLambdaArgumentVisitor aggregate_visitor{"aggregate function", ErrorCodes::UNKNOWN_IDENTIFIER, context, analysis};
    FreeReferencesToLambdaArgumentVisitor window_visitor{"window function", ErrorCodes::UNKNOWN_IDENTIFIER, context, analysis};

    for (auto & aggregate : analysis.getAggregateAnalysis(select_query))
    {
        ASTPtr expr = aggregate.expression;
        traverseExpressionTree(expr, aggregate_visitor, {}, analysis, context);
    }

    for (auto & window : analysis.getWindowAnalysisOfSelectQuery(select_query))
    {
        ASTPtr expr = window->expression;
        traverseExpressionTree(expr, window_visitor, {}, analysis, context);
    }
}


UInt64 QueryAnalyzerVisitor::analyzeUIntConstExpression(const ASTPtr & expression)
{
    auto val = tryEvaluateConstantExpression(expression, context);

    if (!val)
        throw Exception(
            "Element of set in IN, VALUES or LIMIT is not a constant expression: " + serializeAST(*expression), ErrorCodes::BAD_ARGUMENTS);

    auto uint_val = convertFieldToType(*val, DataTypeUInt64());

    return uint_val.safeGet<UInt64>();
}

void QueryAnalyzerVisitor::countLeadingHint(const IAST & ast)
{
    for (const auto & hint : ast.hints)
    {
        if (Poco::toLower(hint.getName()) == "leading")
            ++analysis.hint_analysis.leading_hint_count;
    }
}

void QueryAnalyzerVisitor::rewriteSelectInANSIMode(ASTSelectQuery & select_query, const Aliases & aliases, ScopePtr source_scope)
{
    if (use_ansi_semantic)
    {
        NameSet source_columns_set, source_columns_set_without_ambiguous;
        for (const auto & field : source_scope->getFields())
        {
            const auto & name = field.name;
            if (source_columns_set.count(name))
                source_columns_set_without_ambiguous.erase(name);
            else
                source_columns_set_without_ambiguous.emplace(name);

            source_columns_set.emplace(name);
        }
        if (context->getSettingsRef().prefer_alias_if_column_name_is_ambiguous)
            source_columns_set = std::move(source_columns_set_without_ambiguous);

        QueryNormalizer::Data normalizer_prefer_source_data(
            aliases,
            source_columns_set,
            false, /* ignore_alias */
            context->getSettingsRef(),
            true, /* allow_self_aliases */
            context,
            nullptr, /* storage */
            nullptr, /* metadata_snapshot */
            false /* rewrite_map_col */);
        QueryNormalizer normalizer_prefer_source(normalizer_prefer_source_data);

        QueryNormalizer::Data normalizer_prefer_alias_data(
            aliases,
            source_columns_set,
            false, /* ignore_alias */
            context->getSettingsRef(),
            true, /* allow_self_aliases */
            context,
            nullptr, /* storage */
            nullptr, /* metadata_snapshot */
            false /* rewrite_map_col */);
        normalizer_prefer_alias_data.settings.prefer_column_name_to_alias = false;
        QueryNormalizer normalizer_prefer_alias(normalizer_prefer_alias_data);

        auto select_id = select_query.getTreeHash().first;
        LOG_DEBUG(
            logger, "ANSI alias process, select id {}, before query: {}", select_id, select_query.formatForErrorMessageWithoutAlias());
        LOG_TRACE(logger, "ANSI alias process, select id {}, before ast: {}", select_id, select_query.dumpTree());

        if (select_query.select())
            normalizer_prefer_source.visit(select_query.refSelect());
        if (select_query.where())
            normalizer_prefer_source.visit(select_query.refWhere());
        if (select_query.groupBy())
            normalizer_prefer_source.visit(select_query.refGroupBy());
        if (select_query.having())
        {
            if (context->getSettingsRef().allow_mysql_having_name_resolution)
                resolveNamesInHavingAsMySQL(select_query);
            else
                normalizer_prefer_source.visit(select_query.refHaving());
        }
        if (select_query.window())
            normalizer_prefer_source.visit(select_query.refWindow());
        if (select_query.orderBy())
            normalizer_prefer_alias.visit(select_query.refOrderBy());

        LOG_DEBUG(logger, "ANSI alias process, select id {}, after query: {}", select_id, select_query.formatForErrorMessageWithoutAlias());
        LOG_TRACE(logger, "ANSI alias process, select id {}, after ast: {}", select_id, select_query.dumpTree());
    }
}

void QueryAnalyzerVisitor::normalizeAliases(ASTPtr & expr, ASTPtr & aliases_expr)
{
    Aliases aliases;
    QueryAliasesVisitor(aliases).visit(aliases_expr);
    normalizeAliases(expr, aliases, {});
}

void QueryAnalyzerVisitor::normalizeAliases(ASTPtr & expr, const Aliases & aliases, const NameSet & source_columns_set)
{
    QueryNormalizer::Data normalizer_data(
        aliases,
        source_columns_set,
        false, /* ignore_alias */
        context->getSettingsRef(),
        true, /* allow_self_aliases */
        context,
        nullptr, /* storage */
        nullptr, /* metadata_snapshot */
        false /* rewrite_map_col */);
    QueryNormalizer(normalizer_data).visit(expr);
}

NameSet collectNames(ScopePtr scope)
{
    NameSet result;

    for (const auto & field : scope->getFields())
        result.insert(field.name);

    return result;
}

String
qualifyJoinedName(const String & name, const String & table_qualifier, const NameSet & source_names, bool check_identifier_begin_valid)
{
    if (source_names.count(name) || (check_identifier_begin_valid && !isValidIdentifierBegin(name.at(0))))
        return table_qualifier + name;

    return name;
}

void checkAccess(AnalysisPtr analysis, ContextPtr context)
{
    // check access rights.
    const auto & used_columns = analysis->getUsedColumns();
    for (const auto & [table_id, columns] : used_columns)
    {
        Names required_names(columns.begin(), columns.end());
        context->checkAccess(AccessType::SELECT, table_id, required_names);
    }
    if (used_columns.empty())
    {
        const auto & used_storages = analysis->getStorages();
        auto access = context->getAccess();

        for (const auto & [_, storage_analysis] : used_storages)
        {
            bool is_any_column_granted = false;
            for (const auto & column : storage_analysis.storage->getInMemoryMetadataPtr()->getColumns())
            {
                if (context->isGranted(AccessType::SELECT, storage_analysis.database, storage_analysis.table, column.name))
                {
                    is_any_column_granted = true;
                    break;
                }
            }
            if (!is_any_column_granted)
                throw Exception(
                    ErrorCodes::ACCESS_DENIED,
                    "{}: Not enough privileges. To execute this query it's necessary to have grant SELECT for at least one column on {}",
                    context->getUserName(),
                    storage_analysis.storage->getStorageID().getFullTableName());
        }
    }
}

}
