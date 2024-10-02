#include <Storages/MaterializedView/PartitionTransformer.h>

#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserPartition.h>

#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/executeQuery.h>

#include <Storages/MaterializedView/ApplyPartitionFilterVisitor.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/DataLakes/StorageCnchLakeBase.h>
#include <Storages/Hive/HivePartition.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Analyzers/QueryRewriter.h>
#include <Databases/DatabasesCommon.h>

#include <Optimizer/MaterializedView/ExpressionSubstitution.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <QueryPlan/SymbolMapper.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
    extern const int LOGICAL_ERROR;
}

void PartitionDiff::findPartitions(std::set<String> & partition_names, std::shared_ptr<PartitionDiff> & dest_part_diff)
{
    std::set<String> binary_parts;
    for (const auto & name : partition_names)
    {
        if (part_name_to_binary.find(name) != part_name_to_binary.end())
            binary_parts.insert(part_name_to_binary[name]);
    }
    if (!binary_parts.empty())
    {
        for (const auto & ver_part : add_partitions)
        {
            if (binary_parts.find(ver_part->partition()) != binary_parts.end())
                dest_part_diff->add_partitions.emplace_back(ver_part);
        }
        for (const auto & ver_part : drop_partitions)
        {
            if (binary_parts.find(ver_part->partition()) != binary_parts.end())
                dest_part_diff->drop_partitions.emplace_back(ver_part);
        }
    }
}

VersionPartContainerPtrs PartitionDiff::generatePartitionContainer(const VersionPartPtrs & partitions)
{
    VersionPartContainerPtrs part_containers;
    std::unordered_map<String, VersionPartContainerPtr> part_containers_by_storage;
    for (const auto & ver_part : partitions)
    {
        String id = fmt::format("{}_{}", ver_part->storage_id().database(), ver_part->storage_id().table());
        if (part_containers_by_storage.find(id) == part_containers_by_storage.end())
        {
            VersionPartContainerPtr container(new Protos::VersionedPartitions());
            container->mutable_storage_id()->CopyFrom(ver_part->storage_id());
            auto * versioned_partition = container->add_versioned_partition();
            versioned_partition->mutable_storage_id()->CopyFrom(ver_part->storage_id());
            versioned_partition->set_partition(ver_part->partition());
            versioned_partition->set_last_update_time(ver_part->last_update_time());
            part_containers_by_storage[id] = container;
        }
        else
        {
            auto * versioned_partition = part_containers_by_storage[id]->add_versioned_partition();
            versioned_partition->mutable_storage_id()->CopyFrom(ver_part->storage_id());
            versioned_partition->set_partition(ver_part->partition());
            versioned_partition->set_last_update_time(ver_part->last_update_time());
        }
    }

    for (const auto & container : part_containers_by_storage)
        part_containers.emplace_back(container.second);
    return part_containers;
}

String PartitionDiff::toString() const
{
    std::stringstream ss;
    if (!depend_storage_id.empty())
        ss << "depend_storage_id: " << depend_storage_id.getNameForLogs() << std::endl;

    ss << "add_partitions: [";
    for (const auto & ver_part : add_partitions)
        ss << fmt::format("{}_{}", ver_part->partition(), ver_part->last_update_time()) << ", ";
    ss << "]" << std::endl;

    ss << "drop_partitions: [";
    for (const auto & ver_part : drop_partitions)
        ss << fmt::format("{}_{}", ver_part->partition(), ver_part->last_update_time()) << ", ";
    ss << "]";
    return ss.str();
}

String AsyncRefreshParam::getPartitionMap()
{
    std::stringstream ss;
    for (const auto & relation : part_relation)
        ss << fmt::format("target <{}> -> source <{}>", relation.first, fmt::join(relation.second, ", ")) << std::endl;
    return ss.str();
}

BaseTableInfoPtr PartitionTransformer::getBaseTableInfo(const StorageID & base_table_id)
{
    if (depend_base_tables.find(base_table_id) != depend_base_tables.end())
        return depend_base_tables[base_table_id];
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            fmt::format(" PartitionTransformer::getBaseTableInfo {} is not exist in base tables",base_table_id.getNameForLogs()));
}

void PartitionTransformer::validate(ContextMutablePtr local_context)
{
    if (validated)
        return;
    LOG_DEBUG(log, "PartitionTransformer::validate mv_query: {} ", queryToString(mv_query));
    interpretSettings(mv_query, local_context);
    CurrentThread::get().pushTenantId(local_context->getSettingsRef().tenant_id);
    MaterializedViewStructurePtr structure
        = MaterializedViewStructure::buildFrom(target_table_id, target_table_id, mv_query->clone(), async_materialized_view, local_context);
    validate(local_context, structure);
}

void PartitionTransformer::validate(ContextMutablePtr local_context, MaterializedViewStructurePtr structure)
{
    if (validated)
        return;

    using namespace MaterializedView;

    target_table = structure->target_storage;
    bool enable_non_partition_throw = local_context->getSettingsRef().enable_non_partitioned_base_refresh_throw_exception;
    if (!target_table->getInMemoryMetadataPtr()->hasPartitionKey())
    {
        if (enable_non_partition_throw)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                "materializd view query not support partition based async refresh: target table has no partition key");

        for (const auto & node : structure->outer_sources)
        {
            auto & step = dynamic_cast<TableScanStep &>(*node->getStep());
            base_tables.insert(step.getStorage());
            non_depend_base_tables.insert(step.getStorage()->getStorageID());
        }

        for (const auto & node : structure->inner_sources)
        {
            auto & step = dynamic_cast<TableScanStep &>(*node->getStep());
            base_tables.insert(step.getStorage());
            non_depend_base_tables.insert(step.getStorage()->getStorageID());
        }

        always_non_partition_based = true;
        validated = true;
        return;
    }

    const KeyDescription & target_partition_key_desc = target_table->getInMemoryMetadataPtr()->getPartitionKey();
    target_partition_key_ast = target_partition_key_desc.definition_ast->clone();

    const auto & symbol_map = structure->symbol_map;
    const auto & equivalences_map = structure->expression_equivalences;
    auto output_columns_to_query_columns_map = structure->output_columns_to_query_columns_map;
    SymbolMapper symbol_mapper = SymbolMapper::symbolMapper(output_columns_to_query_columns_map);

    // try extract partition filter
    std::unordered_map<UInt32, std::vector<ConstASTPtr>> predicates_by_table_maps;
    for (const auto & filter : structure->join_hyper_graph.getFilters())
    {
        if (filter.first.count() == 1)
        {
            auto plan_node_id = structure->join_hyper_graph.getPlanNodes(filter.first)[0]->getId();
            predicates_by_table_maps[plan_node_id] = filter.second;
        }
    }

    for (const auto & node : structure->outer_sources)
    {
        auto & step = dynamic_cast<TableScanStep &>(*node->getStep());
        base_tables.insert(step.getStorage());
        non_depend_base_tables.insert(step.getStorage()->getStorageID());
    }

    // check whether mv partition can be calculated from base table partition
    for (const auto & node : structure->inner_sources)
    {
        auto & step = dynamic_cast<TableScanStep &>(*node->getStep());

        base_tables.insert(step.getStorage());
        if (non_depend_base_tables.contains(step.getStorageID()))
            continue;

        // construct rewrite equality map
        std::unordered_set<String> outputs;
        EqualityASTMap<ConstASTPtr> output_columns_map;
        const KeyDescription & source_partition_key_desc = step.getStorage()->getInMemoryMetadataPtr()->getPartitionKey();
        for (size_t i = 0; i < source_partition_key_desc.column_names.size(); i++)
        {
            const auto & column = source_partition_key_desc.column_names[i];
            outputs.emplace(column);
            auto normalize = IdentifierToColumnReference::rewrite(
                step.getStorage().get(), node->getId(), source_partition_key_desc.expression_list_ast->children[i]);
            output_columns_map.emplace(normalizeExpression(normalize, equivalences_map), makeASTIdentifier(column));
        }

        // rewrite expression from mv partition expresion to base table partition expression using equality map
        ASTs assignments;
        for (const auto & expression : target_partition_key_desc.expression_list_ast->children)
        {
            auto normalize = normalizeExpression(symbol_mapper.map(expression), symbol_map, equivalences_map);
            auto rewrite = rewriteExpression(normalize, output_columns_map, outputs);
            if (!rewrite)
            {
                if (depend_base_tables.empty() && log->debug())
                {
                    std::stringstream string;
                    for (const auto & item : output_columns_map)
                        string << queryToString(*item.first) << "->" << queryToString(*item.second) << ";";
                    LOG_DEBUG(log, "rewrite failed expression: {}, column mappings: {}", queryToString(*normalize), string.str());
                }
                break;
            }
            assignments.emplace_back(*rewrite);
        }

        if (assignments.size() == target_partition_key_desc.expression_list_ast->children.size())
        {
            auto table_info = std::make_shared<BaseTableInfo>();

            /// projection epxression
            ASTPtr map_expression_list = std::make_shared<ASTExpressionList>();
            for (const auto & assingment : assignments)
                map_expression_list->children.emplace_back(assingment);
            if (!predicates_by_table_maps[node->getId()].empty())
            {
                ASTs filters;
                for (const auto & filter : predicates_by_table_maps[node->getId()])
                {
                    auto normalize = normalizeExpression(filter, symbol_map, equivalences_map);
                    auto rewrite = rewriteExpression(normalize, output_columns_map, outputs);
                    if (!rewrite)
                        break;
                    filters.emplace_back(*rewrite);
                }

                if (!filters.empty())
                {
                    ASTPtr filter_ast = filters.size() > 1 ? makeASTFunction("and", filters) : *(filters.begin());
                    table_info->filter_pos = map_expression_list->children.size();
                    map_expression_list->children.emplace_back(filter_ast);
                }
            }
            auto syntax_result
                = TreeRewriter(local_context).analyze(map_expression_list, source_partition_key_desc.sample_block.getNamesAndTypesList());
            table_info->transform_expr = ExpressionAnalyzer{map_expression_list, syntax_result, local_context}.getActions(true);

            LOG_DEBUG(log, "based table-{}, partition mapping expression-{}, filter position-{}", step.getStorageID().getNameForLogs(),
                queryToString(map_expression_list), std::to_string(table_info->filter_pos));

            table_info->partition_key_ast = source_partition_key_desc.definition_ast->clone();
            table_info->storage = step.getStorage();
            table_info->unique_id = node->getId();
            depend_base_tables.emplace(step.getStorageID(), table_info);
        }
        else
            non_depend_base_tables.emplace(step.getStorageID());
    }

    for (auto it = depend_base_tables.begin(); it != depend_base_tables.end();)
    {
        if (non_depend_base_tables.contains(it->first))
            it = depend_base_tables.erase(it);
        else
            ++it;
    }

    if (depend_base_tables.empty() && enable_non_partition_throw)
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "materializd view query not support partition based async refresh");
    always_non_partition_based = depend_base_tables.empty();

    validated = true;
}

static void updateVitrualWarehouseSetting(const StoragePtr & target_table, ContextMutablePtr local_context)
{
    String vw_name;
    auto * merge_tree = dynamic_cast<MergeTreeMetaBase *>(target_table.get());
    if (merge_tree && merge_tree->getSettings()->has("cnch_vw_write"))
        local_context->setSetting("virtual_warehouse",  merge_tree->getSettings()->getString("cnch_vw_write"));
}

AsyncRefreshParamPtrs PartitionTransformer::constructRefreshParams(
    PartMapRelations & part_map, PartitionDiffPtr & part_diff, bool combine_params, bool partition_refresh, ContextMutablePtr local_context)
{
    updateVitrualWarehouseSetting(target_table, local_context);
    AsyncRefreshParamPtrs params;
    if (!partition_refresh)
    {
        const auto & settings = local_context->getSettingsRef();
        AsyncRefreshParamPtr refresh_param = std::make_shared<AsyncRefreshParam>();
        auto query = mv_query->clone();
        bool cascading = local_context->getSettingsRef().cascading_refresh_materialized_view;
        auto insert_query = std::make_shared<ASTInsertQuery>();
        insert_query->table_id = target_table_id;
        insert_query->select = query;
        insert_query->children.push_back(insert_query->select);

        if (local_context->getSettingsRef().enable_mv_async_insert_overwrite)
        {
            auto target_current_parts = local_context->getCnchCatalog()->getLastModificationTimeHints(target_table);
            if (!target_current_parts.empty())
            {
                insert_query->is_overwrite = true;
                FormatSettings format_settings;
                std::vector<String> last_target_parts;
                auto & target_meta_base = dynamic_cast<MergeTreeMetaBase &>(*(target_table));
                for (auto & current_part : target_current_parts)
                {
                    ReadBufferFromOwnString read_buffer(current_part.partition_id());
                    auto part_info = std::make_shared<MergeTreePartition>();
                    part_info->load(target_meta_base, read_buffer);
                    WriteBufferFromOwnString write_buffer;
                    part_info->serializeText(target_meta_base, write_buffer, format_settings);
                    last_target_parts.emplace_back(write_buffer.str());
                }
                String patition_list_str = fmt::format("{}", fmt::join(last_target_parts, ","));
                ParserList part_parser{std::make_unique<ParserPartition>(), std::make_unique<ParserToken>(TokenType::Comma), false};
                ASTPtr partition_overwrite_ast = parseQuery(part_parser, patition_list_str, settings.max_query_size, settings.max_parser_depth);
                insert_query->overwrite_partition = partition_overwrite_ast;
                insert_query->children.push_back(partition_overwrite_ast);
                refresh_param->insert_overwrite_query = queryToString(insert_query);
            }
            else
            {
                refresh_param->insert_select_query = queryToString(insert_query);
                refresh_param->drop_partition_query = fmt::format(
                "ALTER TABLE {} {} DROP PARTITION WHERE {}", target_table_id.getFullTableName(), (cascading ? "CASCADING" : ""), "1");
            }
        }
        else
        {
            refresh_param->insert_select_query = queryToString(insert_query);
            refresh_param->drop_partition_query = fmt::format(
                "ALTER TABLE {} {} DROP PARTITION WHERE {}", target_table_id.getFullTableName(), (cascading ? "CASCADING" : ""), "1");
        }
        refresh_param->part_relation = part_map;
        refresh_param->part_diff = part_diff;
        refresh_param->partition_refresh = partition_refresh;
        params.emplace_back(refresh_param);
    }
    else
    {
        auto generate_params = [&](const std::set<String> & target_parts,
                                   const std::set<String> & source_parts,
                                   PartMapRelations & part_relation,
                                   PartitionDiffPtr & partition_diff) -> AsyncRefreshParamPtr {
            AsyncRefreshParamPtr refresh_param = std::make_shared<AsyncRefreshParam>();
            const auto & settings = local_context->getSettingsRef();
            auto query = mv_query->clone();
            QueryRewriter{}.rewrite(query, local_context, false);
            bool cascading = local_context->getSettingsRef().cascading_refresh_materialized_view;
            bool insert_overwrite = local_context->getSettingsRef().enable_mv_async_insert_overwrite;

            /// insert overwrite query
            if (insert_overwrite && !target_parts.empty())
            {
                String patition_list_str = fmt::format("{}", fmt::join(target_parts, ","));
                ParserList part_parser{std::make_unique<ParserPartition>(), std::make_unique<ParserToken>(TokenType::Comma), false};
                ASTPtr partition_overwrite_ast = parseQuery(part_parser, patition_list_str, settings.max_query_size, settings.max_parser_depth);

                if (!source_parts.empty())
                {
                    String in_expresion_str = fmt::format(
                        "{} in ({})",
                        queryToString(depend_base_tables[part_diff->depend_storage_id]->partition_key_ast),
                        fmt::join(source_parts, ","));
                    ParserExpression parser(ParserSettings::CLICKHOUSE);
                    ASTPtr partition_predicate_ast
                        = parseQuery(parser, in_expresion_str, settings.max_query_size, settings.max_parser_depth);
                    ApplyPartitionFilterVisitor::visit(query, part_diff->depend_storage_id, partition_predicate_ast, local_context);
                    LOG_DEBUG(log, "ApplyPartitionFilterVisitor rewrite: {}", queryToString(query));
                }

                auto insert_query = std::make_shared<ASTInsertQuery>();
                insert_query->table_id = target_table_id;
                insert_query->select = query->clone();
                insert_query->is_overwrite = true;
                insert_query->overwrite_partition = partition_overwrite_ast;
                insert_query->children.push_back(insert_query->select);
                insert_query->children.push_back(partition_overwrite_ast);
                refresh_param->insert_overwrite_query = queryToString(insert_query);
            }
            else /// drop partition  + insert select
            {
                if (!source_parts.empty())
                {
                    String in_expresion_str = fmt::format(
                        "{} in ({})",
                        queryToString(depend_base_tables[part_diff->depend_storage_id]->partition_key_ast),
                        fmt::join(source_parts, ","));
                    ParserExpression parser(ParserSettings::CLICKHOUSE);
                    ASTPtr partition_predicate_ast = parseQuery(parser, in_expresion_str, settings.max_query_size, settings.max_parser_depth);

                    /// partition predicate condition
                    ApplyPartitionFilterVisitor::visit(query, part_diff->depend_storage_id, partition_predicate_ast, local_context);
                    LOG_DEBUG(log, "ApplyPartitionFilterVisitor rewriet: {}", queryToString(query));

                    auto insert_query = std::make_shared<ASTInsertQuery>();
                    insert_query->table_id = target_table_id;
                    insert_query->select = query->clone();
                    insert_query->children.push_back(insert_query->select);
                    refresh_param->insert_select_query = queryToString(insert_query);
                }

                if (!target_parts.empty())
                {
                    refresh_param->drop_partition_query = fmt::format(
                        "ALTER TABLE {} {} DROP PARTITION WHERE {} IN ({})",
                        target_table_id.getFullTableName(),
                        (cascading ? "CASCADING" : ""),
                        queryToString(target_partition_key_ast),
                        fmt::join(target_parts, ","));
                }
            }
            refresh_param->part_diff = partition_diff;
            refresh_param->part_relation = part_relation;
            refresh_param->partition_refresh = partition_refresh;
            return refresh_param;
        };

        if (combine_params)
        {
            std::set<String> source_parts;
            std::set<String> target_parts;
            for (const auto & relation : part_map)
            {
                target_parts.insert(relation.first);
                for (const auto & part : relation.second)
                    source_parts.insert(part);
            }
            if (!part_map.empty())
                params.emplace_back(generate_params(target_parts, source_parts, part_map, part_diff));
        }
        else
        {
            for (auto & relation : part_map)
            {
                PartMapRelations one_part_map;
                one_part_map[relation.first] = relation.second;
                PartitionDiffPtr one_part_diff = std::make_shared<PartitionDiff>();
                one_part_diff->depend_storage_id = part_diff->depend_storage_id;
                part_diff->findPartitions(relation.second, one_part_diff);
                params.emplace_back(generate_params({relation.first}, relation.second, one_part_map, one_part_diff));
            }
        }
    }

    for (const auto & param : params)
    {
        LOG_DEBUG(log, "------------------async refresh parameters-----------------------");
        for (const auto & relation : param->part_relation)
            LOG_DEBUG(log, "target-{} <----source-{}", relation.first, fmt::join(relation.second, ","));
        if (!param->drop_partition_query.empty())
            LOG_DEBUG(log, "drop query-{}", param->drop_partition_query);

        if (!param->insert_select_query.empty())
            LOG_DEBUG(log, "insert query-{}", param->insert_select_query);

        if (!param->insert_overwrite_query.empty())
            LOG_DEBUG(log, "insert overwrite-{}", param->insert_overwrite_query);
    }

    return params;
}

PartMapRelations PartitionTransformer::transform(
    const VersionPartPtrs & ver_partitions,
    std::unordered_map<String, String> & name_to_binary,
    const StorageID & base_table_id)
{
    if (always_non_partition_based)
        throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "materializd view query not support partition based async refresh");

    if (ver_partitions.empty())
        return PartMapRelations();
    StoragePtr depend_base_table = depend_base_tables[base_table_id]->storage;
    auto * target_meta_base = dynamic_cast<MergeTreeMetaBase *>(target_table.get());
    auto * source_meta_base = dynamic_cast<MergeTreeMetaBase *>(depend_base_table.get());
    auto * source_lake_table = dynamic_cast<StorageCnchLakeBase *>(depend_base_table.get());
    if (!source_meta_base && !source_lake_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported base table type, source tables only support MergeTree and Hive table");
    if (!target_meta_base)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported target table type, target table only support MergeTree");

    size_t filter_pos = depend_base_tables[base_table_id]->filter_pos;
    /// function: extract merge partition from block
    auto extract_merge_partition = [&](const Block & input_block) -> std::vector<std::shared_ptr<MergeTreePartition>> {
        std::vector<std::shared_ptr<MergeTreePartition>> partition_infos;
        for (size_t row = 0, row_num = input_block.rows(); row < row_num; ++row)
        {
            Row part_row;

            /// when exist filter in partition expression resize block column name
            if (filter_pos != std::numeric_limits<size_t>::max())
                part_row.resize(input_block.columns() - 1);
            else
                part_row.resize(input_block.columns());
            for (size_t col_pos = 0, col_num = input_block.columns(); col_pos < col_num; ++col_pos)
            {
                if (col_pos == filter_pos)
                    continue;
                const auto & partition_column = input_block.getByPosition(col_pos).column;
                partition_column->get(row, part_row[col_pos]);
            }
            partition_infos.emplace_back(std::make_shared<MergeTreePartition>(part_row));
        }
        return partition_infos;
    };

    /// prepare source table input block
    const KeyDescription & depend_table_partition_key_desc = depend_base_table->getInMemoryMetadataPtr()->getPartitionKey();
    Block block = depend_table_partition_key_desc.sample_block.cloneEmpty();

    /// deserialize merge tree partition
    FormatSettings format_settings;
    std::vector<std::shared_ptr<MergeTreePartition>> source_merge_tree_partition_infos;
    std::vector<std::shared_ptr<HivePartition>> source_hive_partition_infos;
    for (const auto & add_partition : ver_partitions)
    {
        StorageID storage_id = RPCHelpers::createStorageID(add_partition->storage_id());
        if (storage_id != depend_base_table->getStorageID())
            continue;
        ReadBufferFromOwnString read_buffer(add_partition->partition());
        if (source_meta_base)
        {
            auto part_info = std::make_shared<MergeTreePartition>();
            part_info->load(*source_meta_base, read_buffer);
            source_merge_tree_partition_infos.emplace_back(part_info);
        }
        else if (source_lake_table)
        {
            auto part_info = std::make_shared<HivePartition>();
            part_info->loadFromBinary(read_buffer, source_lake_table->getInMemoryMetadataPtr()->getPartitionKey());
            source_hive_partition_infos.emplace_back(part_info);
        }
    }

    /// construct expression input block
    MutableColumns columns = block.mutateColumns();
    if (source_meta_base)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            auto & part_column = columns[i];
            for (const auto & info : source_merge_tree_partition_infos)
                part_column->insert(info->value[i]);
        }
    }
    else if (source_lake_table)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            auto & part_column = columns[i];
            for (const auto & info : source_hive_partition_infos)
                part_column->insert(info->value[i]);
        }
    }
    block.setColumns(std::move(columns));

    /// execute partition mapping expression
    depend_base_tables[base_table_id]->transform_expr->execute(block);

    /// construct PartMapRelations <(target_partition) -> (source partition set)>
    std::vector<std::shared_ptr<MergeTreePartition>> target_partition_infos = extract_merge_partition(block);
    PartMapRelations target_to_source_map;
    std::shared_ptr<FilterDescription> filter_des;
    bool pass_all = true;
    if (filter_pos != std::numeric_limits<size_t>::max())
    {
        ConstantFilterDescription const_description(*(block.getByPosition(filter_pos).column));
        if (const_description.always_true)
            pass_all = true;
        else if (const_description.always_false)
            pass_all = false;
        else
            filter_des = std::make_shared<FilterDescription>(*(block.getByPosition(filter_pos).column));
    }

    for (size_t i = 0, rows = target_partition_infos.size(); i < rows; ++i)
    {
        if ((filter_des && (*filter_des->data)[i] == 0) || !pass_all)
            continue;
        WriteBufferFromOwnString target_write_buffer;
        WriteBufferFromOwnString source_write_buffer;
        target_partition_infos[i]->serializeText(*target_meta_base, target_write_buffer, format_settings);
        if (source_meta_base)
            source_merge_tree_partition_infos[i]->serializeText(*source_meta_base, source_write_buffer, format_settings);
        else if (source_lake_table)
            source_hive_partition_infos[i]->serializeText(source_lake_table->getInMemoryMetadataPtr()->getPartitionKey(), source_write_buffer, format_settings);
        String target_str = target_write_buffer.str();
        String source_str = source_write_buffer.str();
        target_to_source_map[target_str].insert(source_str);

        /// Add partition name to binary bytes mapping
        name_to_binary[source_str] = ver_partitions[i]->partition();
    }

    return target_to_source_map;
}

VersionPartPtr PartitionTransformer::convert(const StoragePtr & storage, const Protos::LastModificationTimeHint & part_last_update)
{
    VersionPartPtr versioned_partition(new Protos::VersionedPartition());
    RPCHelpers::fillStorageID(storage->getStorageID(), *versioned_partition->mutable_storage_id());
    versioned_partition->set_partition(part_last_update.partition_id());
    versioned_partition->set_last_update_time(part_last_update.last_modification_time());
    return versioned_partition;
}

String PartitionTransformer::parsePartitionKey(const StoragePtr & storage, String partition_key_binary)
{
    FormatSettings format_settings;
    WriteBufferFromOwnString write_buffer;
    if (auto * meta_base = dynamic_cast<MergeTreeMetaBase *>(storage.get()))
    {
        ReadBufferFromOwnString read_buffer(partition_key_binary);
        auto part_info = std::make_shared<MergeTreePartition>();
        part_info->load(*meta_base, read_buffer);
        part_info->serializeText(*meta_base, write_buffer, format_settings);
    }
    else if (auto * cnch_lake = dynamic_cast<StorageCnchLakeBase *>(storage.get()))
    {
        ReadBufferFromString read_buffer(partition_key_binary);
        StorageMetadataPtr metadata_snapshot = cnch_lake->getInMemoryMetadataPtr();
        auto part_info = std::make_shared<HivePartition>();
        part_info->loadFromBinary(read_buffer, metadata_snapshot->getPartitionKey());
        part_info->serializeText(metadata_snapshot->getPartitionKey(), write_buffer, format_settings);
    }
    return write_buffer.str();
}

}
