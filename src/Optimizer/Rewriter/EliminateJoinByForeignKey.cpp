#include <algorithm>
#include <memory>
#include <unordered_map>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/join_common.h>
#include <Optimizer/DataDependency/DependencyUtils.h>
#include <Optimizer/DataDependency/ForeignKeysTuple.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/Rewriter/EliminateJoinByForeignKey.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Optimizer/makeCastFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/StorageDistributed.h>
#include <common/logger_useful.h>

namespace DB
{

bool EliminateJoinByFK::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    TableColumnInfo info(true);

    PlanNodes table_scan_nodes
        = CollectPlanNodeVisitor::collect(plan.getPlanNode(), {IQueryPlanStep::Type::TableScan}, plan.getCTEInfo(), context);

    std::unordered_map<String, StorageMetadataPtr> metadatas;

    for (const PlanNodePtr & table_ptr : table_scan_nodes)
    {
        String table_name = static_cast<TableScanNode &>(*table_ptr).getStep()->getStorageID().getTableName();
        if (metadatas.contains(table_name))
            continue;
        auto storage = static_cast<TableScanNode &>(*table_ptr).getStep()->getStorage();
        auto distributed_meta = storage->getInMemoryMetadataPtr();
        metadatas.emplace(table_name, distributed_meta);
    }

    for (const auto & [cur_tbl_name, metadata] : metadatas)
        for (const auto & column_name : metadata->getColumns().getAll().getNames())
            info.table_ordinary_columns[cur_tbl_name].emplace(cur_tbl_name, column_name);

    std::unordered_map<String, Names> table_uniques;
    for (const auto & [cur_tbl_name, metadata] : metadatas)
        for (const auto & unique_keys : metadata->getUniqueNotEnforced().getUniqueNames())
            if (unique_keys.size() == 1)
                table_uniques[cur_tbl_name].push_back(unique_keys[0]);

    for (const auto & [cur_tbl_name, metadata] : metadatas)
    {
        auto foreign_keys_desc = metadata->getForeignKeys().getForeignKeysTuple();
        for (const FkToPKPair & fk_pk_pair : foreign_keys_desc)
        {
            if (!metadatas.contains(fk_pk_pair.ref_table_name))
                continue;

            String ref_table_name = fk_pk_pair.ref_table_name;
            if (!metadatas.at(cur_tbl_name)->getColumns().has(fk_pk_pair.fk_column_name))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "foreign key definition error: column not exists - " + cur_tbl_name + '.' + fk_pk_pair.fk_column_name);
            }

            if (!metadatas.at(ref_table_name)->getColumns().has(fk_pk_pair.ref_column_name))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "foreign key definition error: column not exists - " + ref_table_name + '.' + fk_pk_pair.ref_column_name);
            }

            info.table_columns[cur_tbl_name].updateKey(
                ForeignKeyOrPrimaryKey::KeyType::FOREIGN_KEY,
                cur_tbl_name,
                fk_pk_pair.fk_column_name);

            info.table_columns[ref_table_name].updateKey(
                ForeignKeyOrPrimaryKey::KeyType::PRIMARY_KEY,
                ref_table_name,
                fk_pk_pair.ref_column_name);

            info.table_ordinary_columns[ref_table_name].erase(OrdinaryKey{ref_table_name, fk_pk_pair.ref_column_name});

            info.fk_to_pk.emplace(
                TableColumn{cur_tbl_name, fk_pk_pair.fk_column_name}, TableColumn{ref_table_name, fk_pk_pair.ref_column_name});

            auto ref_iter = table_uniques.find(ref_table_name);
            if (ref_iter != table_uniques.end())
            {
                for (const auto & unique_key : ref_iter->second)
                {
                    info.table_columns[ref_table_name].updateKey(
                        ForeignKeyOrPrimaryKey::KeyType::PRIMARY_KEY,
                        ref_table_name,
                        unique_key);

                    info.table_ordinary_columns[ref_table_name].erase(OrdinaryKey{ref_table_name, unique_key});
                }
            }
        }
    }

    if (info.fk_to_pk.empty())
        return false;

    EliminateJoinByFK::Rewriter rewriter{context, plan.getCTEInfo(), info};
    JoinInfo v;
    VisitorUtil::accept(plan.getPlanNode(), rewriter, v);
    rewriter.collectEliminableJoin(v.getWinners());


    for (const auto & winner : rewriter.getFinalWinners())
    {
        std::ostringstream ostr;
        ostr << "table= `" << winner.first << "` ";
        ostr << "winner_id=" << winner.second.winner->getId() << " ";
        ostr << "winner_bottom_joins: ";
        for (const auto & bottom_join : winner.second.bottom_joins)
            ostr << bottom_join.first->getId() << ", ";
        LOG_INFO(getLogger("DataDependency"), "EliminateJoinByFK-JoinInfo. " + ostr.str());

        EliminateJoinByFK::Eliminator eliminator{context, plan.getCTEInfo(), winner, info};
        JoinEliminationContext c;
        auto result = VisitorUtil::accept(plan.getPlanNode(), eliminator, c);
        plan.update(result);
    }
    return !rewriter.getFinalWinners().empty();
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitPlanNode(PlanNodeBase & node, JoinInfo & join_info)
{
    size_t children_size = node.getChildren().size();
    if (children_size != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "unsupported step type -- " + node.getStep()->getName() + ", skip it by setting enable_eliminate_join_by_fk=0");

    FPKeysAndOrdinaryKeys translated = VisitorUtil::accept(node.getChildren()[0], *this, join_info);

    // LOG_INFO(getLogger("DataDependency"), "visitPlanNode=" + std::to_string(node.getId()) + ", winners=" + std::to_string(join_info.getWinners().size()) + ". " + translated.keysStr());

    return translated.clearFPKeys();
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitJoinNode(JoinNode & node, JoinInfo & join_info)
{
    if (node.getStep()->hasKeyIdNullSafe())
        return {};

    std::vector<FPKeysAndOrdinaryKeys> input_keys;

    ForeignKeyOrPrimaryKeys old_common_fp_keys; // only for bottom join.
    std::vector<JoinInfo> join_infos(2);
    for (size_t i = 0; i < 2; i++)
    {
        const auto & child = node.getChildren()[i];
        auto result = VisitorUtil::accept(child, *this, join_infos[i]);
        old_common_fp_keys.insert(result.getFPKeys().begin(), result.getFPKeys().end());
        input_keys.emplace_back(result);
    }

    NameSet common_pk_tables;
    {
        join_info = join_infos[0];
        for (const auto & winner : join_info.getWinners())
        {
            common_pk_tables.insert(winner.first);
        }
        // only can merge same pk table from children, otherwise can't align at pk/fk in eliminating.
        std::erase_if(common_pk_tables, [&](const auto & pk_table){
            return !join_infos[1].getWinners().contains(pk_table);
        });
        join_info = join_info.append(join_infos[1]);
        std::vector<std::pair<String, JoinInfo::JoinWinner>> old_winners = join_info.extractCanRemoveDirectly();
        collectEliminableJoin(old_winners);
    }

    auto & winners = join_info.getWinnersRef();
    const auto & candidate = node.shared_from_this();
    auto step = static_cast<const JoinStep &>(*node.getStep());

    std::unordered_map<String, String> identities;
    for (const auto & item : step.getOutputStream().header)
    {
        identities[item.name] = item.name;
    }

    const auto & left_ordinary_keys = input_keys[0].getOrdinaryKeys();
    const auto & right_ordinary_keys = input_keys[1].getOrdinaryKeys();
    const auto & left_fp_keys = input_keys[0].getFPKeys();
    const auto & right_fp_keys = input_keys[1].getFPKeys();

    FPKeysAndOrdinaryKeys translated;
    {
        OrdinaryKeys common_ordinary_keys;
        common_ordinary_keys.insert(left_ordinary_keys.begin(), left_ordinary_keys.end());
        common_ordinary_keys.insert(right_ordinary_keys.begin(), right_ordinary_keys.end());
        translated.ordinary_keys = common_ordinary_keys;

        ForeignKeyOrPrimaryKeys common_fp_keys;
        common_fp_keys.insert(left_fp_keys.begin(), left_fp_keys.end());
        common_fp_keys.insert(right_fp_keys.begin(), right_fp_keys.end());
        translated.fp_keys = common_fp_keys;
    }

    // LOG_INFO(getLogger("DataDependency"), "visitJoinNode=" + std::to_string(node.getId()) + ", winners=" + std::to_string(join_info.getWinners().size()) + ". " + translated.keysStr());

    bool is_inner_join = step.getKind() == ASTTableJoin::Kind::Inner;
    bool is_outer_join = step.isOuterJoin() && step.getKind() != ASTTableJoin::Kind::Full; // only allow left outer/right outer join.
    bool is_semi_join = (step.getKind() == ASTTableJoin::Kind::Left || step.getKind() == ASTTableJoin::Kind::Right) && step.getStrictness() == ASTTableJoin::Strictness::Semi;

    NameSet invalid_tables;
    // Only one condition in join can be accepted in foreign key dependency optimization.
    if (step.getLeftKeys().size() == 1 && (is_inner_join || is_outer_join || is_semi_join))
    {
        if (!left_fp_keys.empty() && !right_fp_keys.empty())
        {
            TableColumn left_table_column;
            TableColumn right_table_column;

            bool is_left_pk_column = false;
            bool is_right_pk_column = false;

            bool is_left_fk_column = false;
            bool is_right_fk_column = false;

            if (auto keys = left_fp_keys.getPrimaryKeySet().getKeysInCurrentNames(step.getLeftKeys()); !keys.empty())
            {
                is_left_pk_column = true;
                left_table_column = TableColumn{keys.begin()->getTableName(), keys.begin()->getColumnName()};
            }
            if (auto keys = right_fp_keys.getPrimaryKeySet().getKeysInCurrentNames(step.getRightKeys()); !keys.empty())
            {
                is_right_pk_column = true;
                right_table_column = TableColumn{keys.begin()->getTableName(), keys.begin()->getColumnName()};
            }

            if (auto keys = left_fp_keys.getForeignKeySet().getKeysInCurrentNames(step.getLeftKeys()); !keys.empty() && left_table_column.empty())
            {
                is_left_fk_column = true;
                left_table_column = TableColumn{keys.begin()->getTableName(), keys.begin()->getColumnName()};
            }
            if (auto keys = right_fp_keys.getForeignKeySet().getKeysInCurrentNames(step.getRightKeys()); !keys.empty() && right_table_column.empty())
            {
                is_right_fk_column = true;
                right_table_column = TableColumn{keys.begin()->getTableName(), keys.begin()->getColumnName()};
            }

            // cases when cardinaliry of pk table will be changed, we add tbl_name to 'invalid_tables':
            // special: 'pk column' join 'pk column', we always allow it.
            // 'pk column' inner join 'any column which isn't corresponding fk'.
            // 'non-pk column in pk table' inner join 'any column'.

            // 'any column in pk table' right outer join 'any column'.
            // 'any column' left outer join'any column in pk table'.

            // 'any column in pk table' left semi join 'any column'.
            // 'any column' right semi join 'any column in pk table'.

            if (is_left_pk_column && is_right_pk_column)
                ;
            else if (is_inner_join)
            {
                if (is_left_pk_column && is_right_fk_column)
                {
                    auto iter = info.fk_to_pk.find(right_table_column);
                    if (iter != info.fk_to_pk.end() && iter->second == left_table_column)
                        ;
                    else
                        invalid_tables.insert(left_table_column.tbl_name);
                }
                else if (is_left_fk_column && is_right_pk_column)
                {
                    auto iter = info.fk_to_pk.find(left_table_column);
                    if (iter != info.fk_to_pk.end() && iter->second == right_table_column)
                        ;
                    else
                        invalid_tables.insert(right_table_column.tbl_name);
                }
                else
                {
                    invalid_tables.insert(left_table_column.tbl_name);
                    invalid_tables.insert(right_table_column.tbl_name);
                }
            }
            else if (is_outer_join)
            {
                if (step.getKind() == ASTTableJoin::Kind::Left)
                    invalid_tables.insert(right_table_column.tbl_name);
                else
                {
                    assert(step.getKind() == ASTTableJoin::Kind::Right);
                    invalid_tables.insert(left_table_column.tbl_name);
                }
            }
            else
            {
                assert(is_semi_join);

                if (step.getKind() == ASTTableJoin::Kind::Left)
                    invalid_tables.insert(left_table_column.tbl_name);
                else
                {
                    assert(step.getKind() == ASTTableJoin::Kind::Right);
                    invalid_tables.insert(right_table_column.tbl_name);
                }
            }

            {
                // The pk-fk info failure may occur because fk left outer join pk.
                // In this case, the pk table information cannot be passed upward.
                // However, we can check whether only the current join needs to be deleted.

                const auto & fp_keys = old_common_fp_keys;
                // 1. only occur on bottom joins:
                // pk inner join fk: allow and elect
                // fk left outer/semi join pk: allow and elect
                // pk right outer/semi join fk: allow and elect

                ForeignKeyOrPrimaryKeys internal_left_fp_keys = fp_keys.getKeysInCurrentNames(step.getLeftKeys());
                ForeignKeyOrPrimaryKeys internal_right_fp_keys = fp_keys.getKeysInCurrentNames(step.getRightKeys());

                // Check whether the join condition has valid fk and pk.
                String fk_current_name;
                String pk_current_name;
                String pk_table_name;
                String original_pk_name_from_definition;
                int is_fk_in_right = 0;
                // pk join fk
                for (const auto & left_pk : internal_left_fp_keys.getPrimaryKeySet())
                {
                    for (const auto & right_fk : internal_right_fp_keys.getForeignKeySet())
                    {
                        if (step.getKind() == ASTTableJoin::Kind::Right || step.getKind() == ASTTableJoin::Kind::Inner)
                        {
                            if (info.fk_to_pk.at({right_fk.getTableName(), right_fk.getColumnName()})
                                == TableColumn{left_pk.getTableName(), left_pk.getColumnName()})
                            {
                                pk_table_name = left_pk.getTableName();
                                original_pk_name_from_definition = left_pk.getColumnName();
                                fk_current_name = right_fk.getCurrentName();
                                pk_current_name = left_pk.getCurrentName();
                                is_fk_in_right = 1;
                            }
                        }
                    }
                }
                // fk join pk
                for (const auto & right_pk : internal_right_fp_keys.getPrimaryKeySet())
                {
                    for (const auto & left_fk : internal_left_fp_keys.getForeignKeySet())
                    {
                        if (step.getKind() == ASTTableJoin::Kind::Left || step.getKind() == ASTTableJoin::Kind::Inner)
                        {
                            if (info.fk_to_pk.at({left_fk.getTableName(), left_fk.getColumnName()})
                                == TableColumn{right_pk.getTableName(), right_pk.getColumnName()})
                            {
                                pk_table_name = right_pk.getTableName();
                                original_pk_name_from_definition = right_pk.getColumnName();
                                fk_current_name = left_fk.getCurrentName();
                                pk_current_name = right_pk.getCurrentName();
                            }
                        }
                    }
                }

                if (!pk_table_name.empty() && !winners.contains(pk_table_name))
                {
                    std::map<String, String> current_pk_to_fk;
                    for (const auto & pk : fp_keys.getPrimaryKeySet().getCurrentNamesInTable(pk_table_name))
                        current_pk_to_fk.emplace(pk, fk_current_name);

                    winners[pk_table_name].winner = candidate;

                    // a fk-pk join can_removed_directly, iff pk side output_name only contains pure primary key columns.
                    // a pure primary key means it's from fk ref in dll, not unique in dll.
                    auto pk_side_actual_output_names = candidate->getChildren()[1 - is_fk_in_right]->getOutputNames();

                    bool can_remove_directly =
                        std::all_of(pk_side_actual_output_names.begin(), pk_side_actual_output_names.end(), [&](const String & output_name)
                        {
                            return pk_current_name == output_name;
                        });
                    winners[pk_table_name].bottom_joins.emplace(
                        candidate, JoinInfo::JoinWinner::BottomJoinInfo{can_remove_directly, is_fk_in_right, std::move(current_pk_to_fk), original_pk_name_from_definition});
                }
            }

            if (invalid_tables.empty())
            {
                const auto & fp_keys = translated.getFPKeys();

                // 2. only occur on joins above bottom joins:
                // pk inner join pk: allow and elect
                // fk inner join fk: allow and ignore
                // fk inner join other: allow and ignore

                // pk inner join other: impossible
                // other inner join other: impossible
                NameSet converted_pk_tables; // contains pk from fk.
                NameSet invalid_pk_tables;

                NameSet join_keys(step.getLeftKeys().begin(), step.getLeftKeys().end());
                join_keys.insert(step.getRightKeys().begin(), step.getRightKeys().end());
                ForeignKeyOrPrimaryKeys join_fp_keys = fp_keys.getKeysInCurrentNames(join_keys);

                for (const auto & fk : join_fp_keys.getForeignKeySet())
                {
                    TableColumn tbl_col_name{fk.getTableName(), fk.getColumnName()};
                    converted_pk_tables.insert(info.fk_to_pk.at(tbl_col_name).tbl_name);
                }

                NameSet pk_tables;
                for (const auto & pk : join_fp_keys.getPrimaryKeySet())
                {
                    if (!converted_pk_tables.contains(pk.getTableName()) && !invalid_pk_tables.contains(pk.getTableName())) // first step with `pk_1 join pk_2`
                        invalid_pk_tables.emplace(pk.getTableName());
                    else  // `pk_1 join fk_1` or `pk_1 join pk_2`
                    {
                        if (common_pk_tables.contains(pk.getTableName())) // `pk_1 join pk_2`.
                            invalid_pk_tables.erase(pk.getTableName());

                        if (step.getKind() == ASTTableJoin::Kind::Inner)
                            pk_tables.insert(pk.getTableName());
                    }
                }

                for (const String & table_name : pk_tables)
                {
                    if (!invalid_pk_tables.contains(table_name) && winners.contains(table_name))
                    {
                        winners[table_name].winner = candidate;
                    }
                }

                invalid_tables.insert(invalid_pk_tables.begin(), invalid_pk_tables.end());
            }
        }

        // If a child does not have fk information, make the pk table corresponding to ordinary keys in the child invalid.
        // because when pk info is invalidating, pk_keys will be dropped, but ordinary_keys in same table will preserve.
        {
            NameSet partial_invalid_tables;

            for (const auto & key : left_ordinary_keys)
                partial_invalid_tables.insert(key.getTableName());
            for (const auto & key : left_fp_keys)
                partial_invalid_tables.erase(key.getTableName());

            invalid_tables.insert(partial_invalid_tables.begin(), partial_invalid_tables.end());
            partial_invalid_tables.clear();

            for (const auto & key : right_ordinary_keys)
                partial_invalid_tables.insert(key.getTableName());
            for (const auto & key : right_fp_keys)
                partial_invalid_tables.erase(key.getTableName());

            invalid_tables.insert(partial_invalid_tables.begin(), partial_invalid_tables.end());
        }

        NameSet filter_invalid_tables = visitFilterExpression(step.getFilter(), translated);
        invalid_tables.insert(filter_invalid_tables.begin(), filter_invalid_tables.end());
    }
    else
    {
        invalid_tables = translated.downgradeAllPkTables();
    }

    translated.downgradePkTables(invalid_tables);

    std::unordered_map<String, JoinInfo::JoinWinner> old_winners = join_info.reset(invalid_tables, join_infos);
    collectEliminableJoin(old_winners);


    return translated;
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitTableScanNode(TableScanNode & node, JoinInfo &)
{
    ForeignKeyOrPrimaryKeys fp_keys;
    OrdinaryKeys ordinary_keys;
    auto storage = node.getStep()->getStorage();
    auto table_columns = info.table_columns.find(storage->getStorageID().getTableName());
    if (table_columns != info.table_columns.end())
        fp_keys = table_columns->second;

    auto table_ordinary_columns = info.table_ordinary_columns.find(storage->getStorageID().getTableName());
    if (table_ordinary_columns != info.table_ordinary_columns.end())
        ordinary_keys = table_ordinary_columns->second;
    NameToNameMap translation;
    for (const auto & item : node.getStep()->getColumnAlias())
        translation.emplace(item.first, item.second);

    return FPKeysAndOrdinaryKeys{fp_keys, ordinary_keys}.translate(translation);
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitCTERefNode(CTERefNode & node, JoinInfo & c)
{
    auto result = cte_helper.accept(node.getStep()->getId(), *this, c);

    std::unordered_map<String, String> revert_identifies;
    for (const auto & item : node.getStep()->getOutputColumns())
    {
        revert_identifies[item.second] = item.first;
    }

    return result.translate(revert_identifies);
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitExchangeNode(ExchangeNode & node, JoinInfo & c)
{
    return VisitorUtil::accept(node.getChildren()[0], *this, c);
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitSortingNode(SortingNode & node, JoinInfo & c)
{
    return VisitorUtil::accept(node.getChildren()[0], *this, c);
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitAggregatingNode(AggregatingNode & node, JoinInfo & c)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, c);

    auto step = static_cast<const AggregatingStep &>(*node.getStep());

    if (step.getKeys().empty() || !step.isNormal())
        return result.clearFPKeys();

    // fk-pk dependency
    if (!result.getFPKeys().empty())
    {
        NameSet agg_argu_names;
        for (const auto & agg : step.getAggregates())
            agg_argu_names.insert(agg.argument_names.begin(), agg.argument_names.end());

        NameSet invalid_tables;
        // Invalidate tables whose pk column exists in aggregate function.
        for (const auto & pf_key : result.getFPKeys().getPrimaryKeySet().getKeysInCurrentNames(agg_argu_names))
        {
            if (!step.getKeysNotHashed().contains(pf_key.getCurrentName()))
                invalid_tables.insert(pf_key.getTableName());
        }

        for (const auto & ordinary_key : result.getOrdinaryKeys().getKeysInCurrentNames(agg_argu_names))
        {
            if (!step.getKeysNotHashed().contains(ordinary_key.getCurrentName()))
                invalid_tables.insert(ordinary_key.getTableName());
        }

        // Invalidate those tables whose ordinary columns are in the group by keys.
        for (const auto & ordinary_key : result.getOrdinaryKeys().getKeysInCurrentNames(step.getKeys()))
        {
            if (!step.getKeysNotHashed().contains(ordinary_key.getCurrentName()))
                invalid_tables.insert(ordinary_key.getTableName());
        }
        result.downgradePkTables(invalid_tables);
        std::unordered_map<String, JoinInfo::JoinWinner> old_winners = c.reset(invalid_tables, {c});
        collectEliminableJoin(old_winners);
    }

    return result;
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitProjectionNode(ProjectionNode & node, JoinInfo & c)
{
    auto step = static_cast<const ProjectionStep &>(*node.getStep());

    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    std::unordered_map<String, String> revert_identifies;

    for (auto & item : identities)
    {
        revert_identifies[item.second] = item.first;
    }

    auto result = VisitorUtil::accept(node.getChildren()[0], *this, c);

    const auto & old_fp_keys = result.getFPKeys();
    const auto & old_ordinary_keys = result.getOrdinaryKeys();

    FPKeysAndOrdinaryKeys translated = result.translate(revert_identifies);

    const auto & names_and_types = step.getInputStreams()[0].getNamesAndTypes();
    // In special cases, projection adds a assignment function with parameters related to other keys in pk table.
    // we consider the new assignment is a other key.
    // The same is true of aggregating.
    for (const auto & [name, ast] : assignments)
    {
        if (!Utils::isIdentifierOrIdentifierCast(ast))
        {
            NameOrderedSet symbols = SymbolsExtractor::extract(ast);
            NameSet invalid_tables;

            for (const auto & key : old_fp_keys.getKeysInCurrentNames(symbols))
            {
                invalid_tables.insert(key.getTableName());
            }
            for (const auto & key : old_ordinary_keys.getKeysInCurrentNames(symbols))
            {
                invalid_tables.insert(key.getTableName());
            }
            translated.downgradePkTables(invalid_tables);
            std::unordered_map<String, JoinInfo::JoinWinner> old_winners = c.reset(invalid_tables, {c});
            collectEliminableJoin(old_winners);

            continue;
        }

        // cast(pk, 'Nullable(pk_type)') is allowed, it has no effect on pk side.
        if (auto identifier = Utils::tryUnwrapCast(ast, context, names_and_types)->as<ASTIdentifier>())
        {
            auto pks = old_fp_keys.getKeysInCurrentNames(Names{identifier->name()});
            if (!pks.empty())
            {
                translated.getFPKeysRef().insert(pks.begin()->copy(name));
            }
            else
            {
                NameOrderedSet symbols = SymbolsExtractor::extract(ast);
                for (const auto & other_key : old_ordinary_keys.getKeysInCurrentNames(symbols))
                {
                    translated.getOrdinaryKeysRef().updateKey(other_key.getTableName(), name);
                }
            }
        }
    }

    return translated;
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitLimitNode(LimitNode & node, JoinInfo & c)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, c);
    auto invalid_tables = result.downgradeAllPkTables();
    std::unordered_map<String, JoinInfo::JoinWinner> old_winners = c.reset(invalid_tables, {c});
    collectEliminableJoin(old_winners);
    return result;
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitFilterNode(FilterNode & node, JoinInfo & c)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, c);
    auto invalid_tables = visitFilterExpression(node.getStep()->getFilter(), result);

    result.downgradePkTables(invalid_tables);
    std::unordered_map<String, JoinInfo::JoinWinner> old_winners = c.reset(invalid_tables, {c});
    collectEliminableJoin(old_winners);

    return result;
}

FPKeysAndOrdinaryKeys EliminateJoinByFK::Rewriter::visitUnionNode(UnionNode & node, JoinInfo & join_info)
{
    auto step = static_cast<const UnionStep &>(*node.getStep());

    std::vector<FPKeysAndOrdinaryKeys> input_keys;
    size_t children_size = node.getChildren().size();
    std::vector<JoinInfo> join_infos(children_size);

    for (size_t i = 0; i < children_size; i++)
    {
        auto result = VisitorUtil::accept(node.getChildren()[i], *this, join_infos[i]);
        input_keys.emplace_back(result);
    }

    NameSet invalid_tables;
    if (children_size > 0)
    {
        NameSet common_pk_tables;
        join_info = join_infos[0];
        for (const auto & winner : join_info.getWinners())
        {
            common_pk_tables.insert(winner.first);
        }

        for (size_t i = 1; i < children_size; i++)
        {
            // only can merge same pk table from children, otherwise can't align at pk/fk in eliminating.
            std::erase_if(common_pk_tables, [&](const auto & pk_table){
                return !join_infos[i].getWinners().contains(pk_table);
            });
            join_info = join_info.append(join_infos[i]);
        }
        std::vector<std::pair<String, JoinInfo::JoinWinner>> old_winners = join_info.extractCanRemoveDirectly();
        collectEliminableJoin(old_winners);
        // derive difference set of common_pk_tables -- invalid_pk_tables, we should drop them.
        for (size_t i = 0; i < children_size; i++)
        {
            for (const auto & winner : join_infos[i].getWinners())
            {
                if (!common_pk_tables.contains(winner.first))
                    invalid_tables.insert(winner.first);
            }
        }
    }

    std::vector<FPKeysAndOrdinaryKeys> transformed_children_prop;
    const auto & output_to_inputs = step.getOutToInputs();
    size_t index = 0;
    while (index < children_size)
    {
        NameToNameMap mapping;
        for (const auto & output_to_input : output_to_inputs)
        {
            mapping[output_to_input.second[index]] = output_to_input.first;
        }
        transformed_children_prop.emplace_back(input_keys[index].translate(mapping));

        index++;
    }

    FPKeysAndOrdinaryKeys result = transformed_children_prop[0];
    for (size_t i = 1; i < transformed_children_prop.size(); i++)
    {
        std::erase_if(result.getFPKeysRef(), [&](const ForeignKeyOrPrimaryKey & fp_key){
            return !transformed_children_prop[i].getFPKeys().containsByTableColumn(fp_key);
        });
    }

    // LOG_INFO(getLogger("DataDependency"), "visitPlanNode=" + std::to_string(node.getId()) + ", winners=" + std::to_string(join_info.getWinners().size()) + ". " + result.keysStr());

    std::unordered_map<String, JoinInfo::JoinWinner> old_winners = join_info.reset(invalid_tables, join_infos);
    collectEliminableJoin(old_winners);
    result.downgradePkTables(invalid_tables);

    if (!result.getFPKeys().getPrimaryKeySet().empty() && context->getSettingsRef().enable_eliminate_complicated_pk_fk_join_without_top_join)
    {
        // NOTE: Currently `eliminate the join by fk without top join` only supports the union node, because corresponding eliminator must be special rewrite such as Eliminator::visitUnionNode.
        join_info.electMultiChildNode(node.shared_from_this(),result.getFPKeys());
    }

    return result;
}

NameSet EliminateJoinByFK::Rewriter::visitFilterExpression(const ConstASTPtr & filter, FPKeysAndOrdinaryKeys & plan_and_keys)
{
    if (!filter)
        return {};

    if (auto pks = plan_and_keys.getFPKeys().getPrimaryKeySet(); !pks.empty())
    {
        NameSet invalid_tables;

        NamesAndFunctions funcs = CollectExcludeFunction::collect(filter, {}, context);
        for (const auto & func : funcs)
        {
            NameOrderedSet names = SymbolsExtractor::extract(func.second);

            for (const auto & pk : pks)
            {
                NameOrderedSet all_column_names = plan_and_keys.getFPKeys().getCurrentNamesInTable(pk.getTableName());
                auto ordinary_keys = plan_and_keys.getOrdinaryKeys().getCurrentNamesInTable(pk.getTableName());
                all_column_names.insert(ordinary_keys.begin(), ordinary_keys.end());

                // TODO: note pk is now not allowed in filter at present, so eliminator doesn't need to consider mapping the name in filter.
                // if (func.first == "isnotnull" && names.size() == 1 && pk.getCurrentName() == *names.begin()) // done by other rules.
                //     continue;

                if (!getIntersection(all_column_names, names).empty())
                {
                    invalid_tables.insert(pk.getTableName());
                    break;
                }
            }
        }
        return invalid_tables;
    }
    return {};
}




PlanNodePtr EliminateJoinByFK::Eliminator::visitPlanNode(PlanNodeBase & node, JoinEliminationContext & c)
{
    PlanNodes children;
    DataStreams inputs;

    std::vector<JoinEliminationContext> contexts(node.getChildren().size());
    for (size_t i = 0; i < node.getChildren().size(); i++)
    {
        PlanNodePtr child = VisitorUtil::accept(node.getChildren()[i], *this, contexts[i]);
        children.emplace_back(child);
        inputs.push_back(DataStream{child->getCurrentDataStream()});
    }
    if (!contexts.empty())
    {
        c = contexts[0];
        for (size_t i = 1; i < contexts.size(); i++)
        {
            c = c | contexts[i];
        }
    }

    node.getStep()->setInputStreams(inputs);
    node.replaceChildren(children);

    return node.shared_from_this();
}


PlanNodePtr EliminateJoinByFK::Eliminator::visitProjectionNode(ProjectionNode & node, JoinEliminationContext & c)
{
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, c);
    node.replaceChildren({child});

    if (node.getStep()->isFinalProject())
        assert(!c.in_eliminating);

    const auto & assignments = node.getStep()->getAssignments();
    std::unordered_map<String, String> revert_identifies;

    for (auto & item : Utils::computeIdentityTranslations(assignments))
    {
        revert_identifies[item.second] = item.first;
    }
    auto old_pk_to_fk = c.current_pk_to_fk;
    c.translate(revert_identifies, context);

    NameToType name_to_type = node.getStep()->getNameToType();
    // after translating, check is doing between currentName and assignment.first(outputColumns).
    if (c.in_eliminating)
    {
        Assignments new_assignments;
        NameToType new_name_to_type;

        for (const auto & assignment : assignments)
        {
            if (c.current_pk_to_fk.contains(assignment.first))
            {
                auto fk_pair = c.current_pk_to_fk.at(assignment.first);
                if (!new_assignments.contains(fk_pair.first))
                {
                    String old_pk_name = assignment.second->as<ASTIdentifier &>().name();
                    if (old_pk_to_fk.contains(old_pk_name))
                    {
                        new_assignments.emplace(fk_pair.first, std::make_shared<ASTIdentifier>(old_pk_to_fk.at(old_pk_name).first));
                        new_name_to_type[fk_pair.first] = fk_pair.second;
                    }
                }
            }
            else if (!c.current_ordinary_columns.contains(assignment.first))
            {
                new_assignments.emplace_back(assignment);
                new_name_to_type[assignment.first] = name_to_type.at(assignment.first);
            }
        }
        for (const auto & header : c.additional_fk_for_multi_child_node)
        {
            if (!new_assignments.contains(header.name))
            {
                new_assignments.emplace(header.name, std::make_shared<ASTIdentifier>(header.name));
                new_name_to_type[header.name] = header.type;
            }
        }

        // if empty project return child node.
        if (new_assignments.empty())
            return child;
        auto expr_step = std::make_shared<ProjectionStep>(
            child->getStep()->getOutputStream(),
            new_assignments,
            new_name_to_type,
            node.getStep()->isFinalProject(),
            node.getStep()->isIndexProject(),
            node.getStep()->getHints());
        PlanNodes children{child};
        auto expr_node = ProjectionNode::createPlanNode(context->nextNodeId(), std::move(expr_step), children);
        return expr_node;
    }

    return node.shared_from_this();
}

PlanNodePtr EliminateJoinByFK::Eliminator::visitJoinNode(JoinNode & node, JoinEliminationContext & c)
{
    PlanNodes children;
    DataStreams inputs;

    std::vector<JoinEliminationContext> contexts(node.getChildren().size());
    for (size_t i = 0; i < node.getChildren().size(); i++)
    {
        PlanNodePtr child = VisitorUtil::accept(node.getChildren()[i], *this, contexts[i]);
        children.emplace_back(child);
        inputs.push_back(DataStream{child->getCurrentDataStream()});
    }
    if (!contexts.empty())
    {
        c = contexts[0];
        for (size_t i = 1; i < contexts.size(); i++)
        {
            c = c | contexts[i];
        }
    }

    node.getStep()->setInputStreams(inputs);
    node.replaceChildren(children);

    auto step = node.getStep();

    if (!c.in_eliminating)
    {
        const auto & bottom_joins = winner.second.bottom_joins;
        if (auto iter = std::find_if(
                bottom_joins.begin(), bottom_joins.end(), [&](const auto & node_ptr) { return node_ptr.first->getId() == node.getId(); });
            iter != bottom_joins.end())
        {
            // start eliminating.
            c.in_eliminating = true;
            auto name_to_type = iter->first->getChildren()[iter->second.is_fk_in_right]->getOutputNamesToTypes();
            for (const auto & [pk, fk] : iter->second.current_pk_to_fk)
            {
                if (!name_to_type.contains(fk))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "key not found when eliminateing bottom join - " + fk);
                c.current_pk_to_fk.emplace(pk, std::pair<String, DataTypePtr>(fk, name_to_type.at(fk)));
            }

            int is_fk_in_right = iter->second.is_fk_in_right;

            // add isNotNull filter on the fk side.
            PlanNodePtr fk_node = node.getChildren()[is_fk_in_right];

            String current_fk_name = c.current_pk_to_fk.begin()->second.first;

            if (winner.second.winner->getType() != IQueryPlanStep::Type::Join)
            {
                if (is_fk_in_right == 0)
                {
                    for (const auto & header : node.getChildren()[0]->getOutputNamesAndTypes())
                    {
                        if (header.name == step->getLeftKeys()[0])
                        {
                            c.additional_fk_for_multi_child_node.push_back(header);
                            break;
                        }
                    }
                }
                else
                {
                    for (const auto & header : node.getChildren()[1]->getOutputNamesAndTypes())
                    {
                        if (header.name == step->getRightKeys()[0])
                        {
                            c.additional_fk_for_multi_child_node.push_back(header);
                            break;
                        }
                    }
                }
                assert(c.additional_fk_for_multi_child_node.size() == 1);
            }

            PlanNodePtr child_node = fk_node;

            // check whether need is_not_null filter on fk side.
            auto kind = node.getStep()->getKind();
            if ((kind != ASTTableJoin::Kind::Left && kind != ASTTableJoin::Kind::Right) || node.getStep()->getStrictness() == ASTTableJoin::Strictness::Semi)
            {
                auto filter_step = std::make_shared<FilterStep>(fk_node->getCurrentDataStream(), makeASTFunction("isNotNull", std::make_shared<ASTIdentifier>(current_fk_name)));
                child_node = FilterNode::createPlanNode(context->nextNodeId(), std::move(filter_step), {fk_node});
            }

            // extract filter from the join.
            if (auto join_filter = node.getStep()->getFilter(); join_filter && join_filter != PredicateConst::TRUE_VALUE)
            {
                auto filter_step = std::make_shared<FilterStep>(child_node->getCurrentDataStream(), join_filter);
                child_node = FilterNode::createPlanNode(context->nextNodeId(), std::move(filter_step), {child_node});
            }

            if (bottom_joins.begin()->first->getId() == winner.second.winner->getId()) // if can remove directly.
            {
                return createNewProjectionThenEnd(child_node, node.getCurrentDataStream().getNamesAndTypes(), c);
            }

            return child_node;
        }
        return node.shared_from_this();
    }

    auto left_keys = step->getLeftKeys();
    auto right_keys = step->getRightKeys();

    // 0. find fk_name as join left keys. only useful if current_join is top_join.
    String fk_name;
    for (auto & key : left_keys)
    {
        if (auto iter = c.current_pk_to_fk.find(key); iter != c.current_pk_to_fk.end())
        {
            key = iter->second.first;
            fk_name = iter->second.first;
        }
    }

    for (auto & key : right_keys)
    {
        if (auto iter = c.current_pk_to_fk.find(key); iter != c.current_pk_to_fk.end())
        {
            key = iter->second.first;
            fk_name = iter->second.first;
        }
    }

    ColumnsWithTypeAndName output_header;
    for (const auto & input_stream : inputs)
    {
        for (const auto & header : input_stream.header.getColumnsWithTypeAndName())
        {
            if (c.current_pk_to_fk.contains(header.name))
            {
                auto pair = c.current_pk_to_fk.at(header.name);
                ColumnWithTypeAndName new_header{header.column, pair.second, pair.first};
                output_header.emplace_back(new_header);
            }
            else if (!c.current_ordinary_columns.contains(header.name))
            {
                output_header.emplace_back(header);
            }
        }
    }

    auto join_step = std::make_shared<JoinStep>(
        inputs,
        DataStream{output_header},
        step->getKind(),
        step->getStrictness(),
        step->getMaxStreams(),
        step->getKeepLeftReadInOrder(),
        left_keys,
        right_keys,
        step->getKeyIdsNullSafe(),
        step->getFilter(),
        step->isHasUsing(),
        step->getRequireRightKeys(),
        step->getAsofInequality(),
        step->getDistributionType(),
        step->getJoinAlgorithm(),
        step->isMagic(),
        step->isOrdered(),
        step->isSimpleReordered(),
        step->getRuntimeFilterBuilders(),
        step->getHints());

    auto current_join_node = JoinNode::createPlanNode(node.getId(), std::move(join_step), children);
    // TODO@lijinzhi.zx: According to 'step->getOutputStream().header' to determine which columns to output, when all pk table columns do not need to output, you can not fill join.
    if (winner.second.winner->getId() == node.getId())
    {
        NamesAndTypes expected_header = step->getOutputStream().header.getNamesAndTypes();
        return createNewJoinThenEnd(fk_name, current_join_node, expected_header, c);
    }

    return current_join_node;
}

PlanNodePtr EliminateJoinByFK::Eliminator::visitTableScanNode(TableScanNode & node, JoinEliminationContext & c)
{
    NameToNameMap translation;
    for (const auto & item : node.getStep()->getColumnAlias())
        translation.emplace(item.first, item.second);

    if (winner.first == node.getStep()->getTable())
    {
        c.current_pk_to_original_name = pk_columns_in_pk_table;
        c.current_ordinary_columns = original_ordinary_columns_in_pk_table;
    }
    c.translate(translation, context);

    return node.shared_from_this();
}

PlanNodePtr EliminateJoinByFK::Eliminator::visitFilterNode(FilterNode & node, JoinEliminationContext & c)
{
    return visitPlanNode(node, c);
}

PlanNodePtr EliminateJoinByFK::Eliminator::visitAggregatingNode(AggregatingNode & n, JoinEliminationContext & c)
{
    AggregatingNode & node = static_cast<AggregatingNode &>(*visitPlanNode(n, c));
    if (node.getStep()->getKeys().empty())
        return node.shared_from_this();

    const auto * step = node.getStep().get();

    if (c.in_eliminating)
    {
        Names new_keys;

        for (const auto & key : step->getKeys())
        {
            if (c.current_pk_to_fk.contains(key) && std::find(new_keys.begin(), new_keys.end(), c.current_pk_to_fk.at(key).first) == new_keys.end()) // convert `group by pk` to `group by fk`.
                new_keys.push_back(c.current_pk_to_fk.at(key).first);
            else if (!step->getKeysNotHashed().contains(key)) // remove keys not participate in calculating.
                new_keys.push_back(key);
        }

        auto input_stream = step->getInputStreams().at(0);

        if (!c.additional_fk_for_multi_child_node.empty())
        {
            String fk_name = c.additional_fk_for_multi_child_node[0].name;
            if (!node.getCurrentDataStream().getNamesToTypes().contains(fk_name) && std::find(new_keys.begin(), new_keys.end(), fk_name) == new_keys.end())
            {
                new_keys.push_back(fk_name);
                input_stream.header.insert(ColumnWithTypeAndName{c.additional_fk_for_multi_child_node[0].type, fk_name});
            }
        }

        auto agg_step = std::make_shared<AggregatingStep>(
            input_stream,
            new_keys,
            NameSet{},
            step->getAggregates(),
            step->getGroupingSetsParams(),
            step->isFinal(),
            step->getGroupBySortDescription(),
            step->getGroupings(),
            false,
            step->shouldProduceResultsInOrderOfBucketNumber(),
            step->isNoShuffle(),
            step->isStreamingForCache(),
            step->getHints());

        return AggregatingNode::createPlanNode(context->nextNodeId(), std::move(agg_step), node.getChildren());
    }

    return node.shared_from_this();
}

PlanNodePtr EliminateJoinByFK::Eliminator::createNewProjectionThenEnd(PlanNodePtr child_node, const NamesAndTypes & expected_header, JoinEliminationContext & c)
{
    /**
    //      new_projection(new_assignments = fk:=pk)
    //             |
    //        child_node
    */
    Assignments new_assignments;
    NameToType new_name_to_type;

    for (const auto & [name, type] : expected_header)
    {
        if (c.current_pk_to_fk.contains(name))
        {
            String output_pk_name = c.current_pk_to_fk.at(name).first;
            new_assignments.emplace(name, std::make_shared<ASTIdentifier>(output_pk_name));
            new_name_to_type[name] = type;
        }
        else
        {
            new_assignments.emplace(name, std::make_shared<ASTIdentifier>(name));
            new_name_to_type[name] = type;
        }
    }

    auto new_projection_step
        = std::make_shared<ProjectionStep>(child_node->getCurrentDataStream(), new_assignments, new_name_to_type);

    c.clear();
    return ProjectionNode::createPlanNode(context->nextNodeId(), std::move(new_projection_step), {child_node});
}

PlanNodePtr EliminateJoinByFK::Eliminator::createNewJoinThenEnd(const String & fk_name, PlanNodePtr current_node, const NamesAndTypes & expected_header, JoinEliminationContext & c)
{
    /*
    //            new_projection
    //                  |
    //               new_join
    //            /              \
    // current_node(join/union) new_table_scan
    */

    auto first_bottom_join = *std::min_element(winner.second.bottom_joins.begin(), winner.second.bottom_joins.end(),
        [](const auto & pair, const auto & pair2){ return pair.first->getId() < pair2.first->getId(); });

    // 1. create new_table_scan_node from erased bottom_table_scan_node, and fill column_alias with origin columns.
    PlanNodes table_scan_nodes = CollectPlanNodeVisitor::collect(
        first_bottom_join.first, {IQueryPlanStep::Type::TableScan}, cte_helper.getCTEInfo(), context);
    PlanNodePtr table_scan_ptr;
    for (const PlanNodePtr & table_ptr : table_scan_nodes)
    {
        String table_name = static_cast<TableScanNode &>(*table_ptr).getStep()->getStorageID().getTableName();
        if (table_name == winner.first)
        {
            table_scan_ptr = table_ptr;
            break;
        }
    }
    assert(table_scan_ptr);

    auto tb_step = static_cast<TableScanNode &>(*table_scan_ptr).getStep();

    NamesWithAliases column_alias;
    for (const auto & column : tb_step->getStorage()->getInMemoryMetadataPtr()->getColumns().getAll())
    {
        column_alias.emplace_back(column.name, context->getSymbolAllocator()->newSymbol(column.name));
    }

    auto new_table_scan_node = TableScanNode::createPlanNode(
        context->nextNodeId(),
        std::make_shared<TableScanStep>(
            context,
            tb_step->getStorageID(),
            column_alias,
            tb_step->getQueryInfo(),
            tb_step->getMaxBlockSize(),
            tb_step->getTableAlias(),
            tb_step->isBucketScan(),
            tb_step->getHints(),
            tb_step->getInlineExpressions(),
            tb_step->getPushdownAggregation(),
            tb_step->getPushdownProjection(),
            tb_step->getPushdownFilter()));

    // 2. find pk_name as join right keys.
    NameToNameMap table_scan_translation;
    for (const auto & item : static_cast<const TableScanStep &>(*new_table_scan_node->getStep()).getColumnAlias())
        table_scan_translation.emplace(item.first, item.second);

    String pk_name = table_scan_translation.at(first_bottom_join.second.original_pk_name_from_definition);

    // 3. create new_join_node.
    DataStreams new_inputs{current_node->getCurrentDataStream(), new_table_scan_node->getCurrentDataStream()};

    DataTypePtr left_type = new_inputs[0].getNamesToTypes().at(fk_name);
    DataTypePtr right_type = new_inputs[1].getNamesToTypes().at(pk_name);

    // 3-1. add new projection at pk side with cast function.
    if (!JoinCommon::isJoinCompatibleTypes(left_type, right_type))
    {
        auto common_type = getLeastSupertype(DataTypes{left_type, right_type}, context->getSettingsRef().allow_extended_type_conversion);
        if (!common_type->equals(*left_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "EliminateByForeignKey::Eliminator logical error! fk type isn't the leastSupertType of pk type.");
        auto cast_function = makeCastFunction(std::make_shared<ASTIdentifier>(pk_name), common_type);

        Assignments new_assignments;
        NameToType new_name_to_type;

        for (const auto & [name, type] : new_table_scan_node->getCurrentDataStream().getNamesAndTypes())
        {
            if (name == pk_name)
            {
                new_assignments.emplace(name, cast_function);
                new_name_to_type[name] = common_type;
            }
            else
            {
                new_assignments.emplace(name, std::make_shared<ASTIdentifier>(name));
                new_name_to_type[name] = type;
            }
        }

        auto new_projection_step
            = std::make_shared<ProjectionStep>(new_table_scan_node->getCurrentDataStream(), new_assignments, new_name_to_type);

        new_table_scan_node = ProjectionNode::createPlanNode(context->nextNodeId(), std::move(new_projection_step), {new_table_scan_node});
    }

    ColumnsWithTypeAndName new_output_header = new_inputs[0].header.getColumnsWithTypeAndName();
    new_output_header.insert(
        new_output_header.end(),
        new_inputs[1].header.getColumnsWithTypeAndName().begin(),
        new_inputs[1].header.getColumnsWithTypeAndName().end());

    auto new_join_step = std::make_shared<JoinStep>(
        new_inputs,
        DataStream{new_output_header},
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        static_cast<JoinStep & >(*first_bottom_join.first->getStep()).getMaxStreams(),
        static_cast<JoinStep & >(*first_bottom_join.first->getStep()).getKeepLeftReadInOrder(),
        Names{fk_name},
        Names{pk_name});

    auto new_join_node
        = JoinNode::createPlanNode(context->nextNodeId(), std::move(new_join_step), {current_node, new_table_scan_node});

    // 4. create new_projection_node.
    Assignments new_assignments;
    NameToType new_name_to_type;

    for (const auto & [name, type] : expected_header)
    {
        if (c.current_pk_to_fk.contains(name))
        {
            String output_pk_name = table_scan_translation.at(c.current_pk_to_original_name.at(name));
            new_assignments.emplace(name, std::make_shared<ASTIdentifier>(output_pk_name));
            new_name_to_type[name] = type;
        }
        else if (c.current_ordinary_columns.contains(name))
        {
            String src_name = table_scan_translation.at(c.current_ordinary_columns.at(name));
            new_assignments.emplace(name, std::make_shared<ASTIdentifier>(src_name));
            new_name_to_type[name] = type;
        }
        else
        {
            new_assignments.emplace(name, std::make_shared<ASTIdentifier>(name));
            new_name_to_type[name] = type;
        }
    }

    auto new_projection_step
        = std::make_shared<ProjectionStep>(new_join_node->getCurrentDataStream(), new_assignments, new_name_to_type);


    // end eliminating.
    c.clear();

    return ProjectionNode::createPlanNode(context->nextNodeId(), std::move(new_projection_step), {new_join_node});
}

PlanNodePtr EliminateJoinByFK::Eliminator::visitUnionNode(UnionNode & node, JoinEliminationContext & c)
{
    PlanNodes children;
    DataStreams inputs;

    std::vector<JoinEliminationContext> contexts(node.getChildren().size());
    for (size_t i = 0; i < node.getChildren().size(); i++)
    {
        PlanNodePtr child = VisitorUtil::accept(node.getChildren()[i], *this, contexts[i]);
        children.emplace_back(child);
        inputs.push_back(DataStream{child->getCurrentDataStream()});
    }
    if (!contexts.empty())
    {
        c =  contexts[0];
        for (size_t i = 1; i < contexts.size(); i++)
        {
            c = c | contexts[i];
        }
    }

    const auto * step = node.getStep().get();
    NamesAndTypes expected_header = step->getOutputStream().header.getNamesAndTypes(); // for creating new projection.

    node.getStep()->setInputStreams(inputs);
    node.replaceChildren(children);

    if (c.in_eliminating)
    {
        NameToNameMap mapping;
        for (size_t index = 0; index < node.getChildren().size(); index++)
        {
            for (const auto & output_to_input : node.getStep()->getOutToInputs())
            {
                mapping[output_to_input.second[index]] = output_to_input.first;
            }
        }
        auto old_pk_to_fk = c.current_pk_to_fk;
        c.translate(mapping, context);

        DataStream output_stream;
        std::unordered_map<String, std::vector<String>> output_to_inputs;
        for (const auto & item : step->getOutputStream().header)
        {
            if (auto iter = c.current_pk_to_fk.find(item.name); iter != c.current_pk_to_fk.end())
            {
                output_stream.header.insert(ColumnWithTypeAndName{item.column, iter->second.second, iter->second.first});
                std::vector<String> input_names;
                for (const auto & input : step->getOutToInputs().at(item.name))
                    input_names.emplace_back(old_pk_to_fk.at(input).first);
                output_to_inputs[iter->second.first] = input_names;
            }
            else if (!c.current_ordinary_columns.contains(item.name))
            {
                output_stream.header.insert(item);
                output_to_inputs[item.name] = step->getOutToInputs().at(item.name);
            }
        }

        String fk_name;
        if (winner.second.winner->getId() == node.getId())
        {
            // Complete fk header if missing.
            assert(contexts[0].additional_fk_for_multi_child_node.size() == 1);
            if (!std::all_of(contexts.begin(), contexts.end(), [](const JoinEliminationContext & cc){ return cc.additional_fk_for_multi_child_node.size() == 1; }))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "EliminateByForeignKey::Eliminator logic assert failed!");
            const auto & header = c.additional_fk_for_multi_child_node[0];
            if (!output_to_inputs.contains(header.name))
            {
                std::vector<String> input_headers;
                input_headers.reserve(contexts.size());
                for (const auto & pair : contexts)
                    input_headers.push_back(pair.additional_fk_for_multi_child_node[0].name);
                fk_name = context->getSymbolAllocator()->newSymbol(header.name);
                output_to_inputs[fk_name] = input_headers;
                output_stream.header.insert(ColumnWithTypeAndName{header.type, fk_name});
            }
            else
            {
                fk_name = header.name;
            }
        }
        auto union_step = std::make_shared<UnionStep>(inputs, output_stream, output_to_inputs, step->getMaxThreads(), step->isLocal());
        // at present, just reuse current node.getId(), because mutli-winner save the same nodeId, change nodeId may render latter winner failed.
        auto current_union_node = UnionNode::createPlanNode(node.getId(), std::move(union_step), children);

        if (winner.second.winner->getId() == node.getId())
        {
            return createNewJoinThenEnd(fk_name, current_union_node, expected_header, c);
        }
        return current_union_node;
    }
    return node.shared_from_this();
}

PlanNodePtr EliminateJoinByFK::Eliminator::visitCTERefNode(CTERefNode & node, JoinEliminationContext & c)
{
    auto result = cte_helper.accept(node.getStep()->getId(), *this, c);

    std::unordered_map<String, String> revert_identifies;
    for (const auto & item : node.getStep()->getOutputColumns())
    {
        revert_identifies[item.second] = item.first;
    }

    auto old_current_pk_to_fk = c.current_pk_to_fk;
    c.translate(revert_identifies, context);

    if (c.in_eliminating)
    {
        NamesAndTypes result_columns;
        for (const auto & item : node.getStep()->getOutputStream().header.getNamesAndTypes())
        {
            if (c.current_pk_to_fk.contains(item.name))
                result_columns.emplace_back(
                    NameAndTypePair{c.current_pk_to_fk.at(item.name).first, c.current_pk_to_fk.at(item.name).second});
            else if (!c.current_ordinary_columns.contains(item.name))
                result_columns.emplace_back(item);
        }

        std::unordered_map<String, String> output_columns;
        for (const auto & item : node.getStep()->getOutputColumns())
        {
            if (c.current_pk_to_fk.contains(item.first))
                output_columns.emplace(c.current_pk_to_fk.at(item.first).first, old_current_pk_to_fk.at(item.second).first);
            else if (!c.current_ordinary_columns.contains(item.first))
                output_columns.emplace(item);
        }

        cte_helper.getCTEInfo().getCTEDef(node.getStep()->getId()) = result;

        auto new_cte_step = std::make_shared<CTERefStep>(
            DataStream{result_columns}, node.getStep()->getId(), output_columns, node.getStep()->hasFilter());
        return CTERefNode::createPlanNode(context->nextNodeId(), std::move(new_cte_step), {});
    }
    return node.shared_from_this();
}


}
