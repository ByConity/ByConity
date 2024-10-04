#pragma once


#include <unordered_map>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/DataDependency/DataDependency.h>
#include <Optimizer/DataDependency/DataDependencyDeriver.h>
#include <Optimizer/DataDependency/ForeignKeysTuple.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include "QueryPlan/PlanNode.h"
#include "Storages/SelectQueryInfo.h"

namespace DB
{
struct FPKeysAndOrdinaryKeys
{
    ForeignKeyOrPrimaryKeys fp_keys;
    OrdinaryKeys ordinary_keys;

    const ForeignKeyOrPrimaryKeys & getFPKeys() const { return fp_keys; }

    const OrdinaryKeys & getOrdinaryKeys() const { return ordinary_keys; }

    ForeignKeyOrPrimaryKeys & getFPKeysRef() { return fp_keys; }

    OrdinaryKeys & getOrdinaryKeysRef() { return ordinary_keys; }

    FPKeysAndOrdinaryKeys clearFPKeys() const { return FPKeysAndOrdinaryKeys{{}, ordinary_keys}; }

    void downgradePkTables(const NameSet & invalid_tables_) { fp_keys.downgrade(invalid_tables_); }

    NameSet downgradeAllPkTables() { return fp_keys.downgradeAll(); }

    String keysStr() const
    {
        String content = "fp_keys: ";
        for (const auto & fk : fp_keys)
            content += fk.getTableName() + "." + fk.getCurrentName() + ", ";
        content += "ordinary_keys: ";
        for (const auto & ordinary_key : ordinary_keys)
            content += ordinary_key.getTableName() + "." + ordinary_key.getCurrentName() + ", ";

        return content;
    }

    FPKeysAndOrdinaryKeys translate(const NameToNameMap & identities) const
    {
        return FPKeysAndOrdinaryKeys{fp_keys.translate(identities), ordinary_keys.translate(identities)};
    }
};

class EliminateJoinByFK : public Rewriter
{
public:
    String name() const override { return "EliminateJoinByFK"; }

private:
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_eliminate_join_by_fk && !context->getSettingsRef().join_using_null_safe;
    }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;

    class Rewriter;
    class Eliminator;
};

class JoinInfo
{
public:
    struct JoinWinner
    {
        PlanNodePtr winner = nullptr;

        struct BottomJoinInfo
        {
            bool can_remove_directly = false; // Whether remove the bottom join directly and do not add a new join.
            int is_fk_in_right = 0;
            // "current" here means that it can be translated.
            std::map<String, String> current_pk_to_fk;
            // "original" here means real name in table ddl.
            String original_pk_name_from_definition;
        };

        std::unordered_map<PlanNodePtr, BottomJoinInfo> bottom_joins;
    };

    JoinInfo & append(const JoinInfo & other)
    {
        for (const auto & right_winner_iter : other.winners)
        {
            const auto & right_winner = right_winner_iter.second;
            auto left_winner_iter = winners.insert(right_winner_iter);
            if (!left_winner_iter.second)
            {
                auto & left_winner = left_winner_iter.first->second;
                // if exists many pk-to-fk which tbl_name is same but original_pk_name_from_definition is different, drop the latter.
                if (std::all_of(right_winner.bottom_joins.begin(), right_winner.bottom_joins.end(), [&](const auto & pair) {
                        return pair.second.original_pk_name_from_definition
                            == left_winner.bottom_joins.begin()->second.original_pk_name_from_definition;
                    }))
                {
                    left_winner.bottom_joins.insert(right_winner.bottom_joins.begin(), right_winner.bottom_joins.end());
                }
            }
        }
        return *this;
    }

    auto extractCanRemoveDirectly()
    {
        std::vector<std::pair<String, JoinInfo::JoinWinner>> old_winners;
        for (auto & winner : winners)
        {
            const auto & bottom_joins = winner.second.bottom_joins;
            if (std::all_of(bottom_joins.begin(), bottom_joins.end(), [&](const auto & pair) { return pair.second.can_remove_directly; }))
            {
                // split out children, and invalidate pk.
                for (const auto & bottom_join : bottom_joins)
                {
                    old_winners.push_back({winner.first, JoinInfo::JoinWinner{.winner = bottom_join.first, .bottom_joins = {bottom_join}}});
                }
                // clear means invalidate, because it can't pass check in collectEliminableJoin().
                winner.second.bottom_joins.clear();
            }
        }

        return old_winners;
    }

    // MultiChildNode can force Elect as top node, so we can add new join above the Union Node if no useful top join.
    void electMultiChildNode(PlanNodePtr candidate, const ForeignKeyOrPrimaryKeys & fp_keys)
    {
        for (const auto & fp_key : fp_keys.getPrimaryKeySet())
        {
            if (winners.contains(fp_key.getTableName()))
            {
                winners.at(fp_key.getTableName()).winner = candidate;
            }
        }
    }

    const auto & getWinners() const { return winners; }

    auto & getWinnersRef() { return winners; }

    std::unordered_map<String, JoinWinner> reset(const NameSet invalid_tables, const std::vector<JoinInfo> & source_join_infos)
    {
        if (invalid_tables.empty())
            return {};

        std::unordered_map<String, JoinWinner> result;

        std::unordered_map<String, std::vector<JoinWinner>> source_winners;
        for (const auto & source_join_info : source_join_infos)
            for (const auto & source_winner : source_join_info.getWinners())
                source_winners[source_winner.first].push_back(source_winner.second);

        for (auto iter = winners.begin(); iter != winners.end();)
        {
            if (invalid_tables.contains(iter->first) && !iter->second.bottom_joins.empty())
            {
                if (!source_winners.contains(iter->first))
                    result.insert(*iter);
                else // when current winner is generated by merging children join_infos(That is, the corresponding pk table cannot pass the check of the current join), we add the source join_info to final winners.
                    for (const auto & source_winner : source_winners.at(iter->first))
                        result.emplace(iter->first, source_winner);
                iter = winners.erase(iter);
            }
            else
            {
                ++iter;
            }
        }

        return result;
    }

private:
    // map: pk tbl_name -> Join Winner Info
    std::unordered_map<String, JoinWinner> winners;
};

class EliminateJoinByFK::Rewriter : public PlanNodeVisitor<FPKeysAndOrdinaryKeys, JoinInfo>
{
public:
    explicit Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_, const TableColumnInfo & info_)
        : context(context_), cte_helper(cte_info_), info(info_)
    {
    }
    FPKeysAndOrdinaryKeys visitPlanNode(PlanNodeBase &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitJoinNode(JoinNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitTableScanNode(TableScanNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitCTERefNode(CTERefNode &, JoinInfo &) override;

    FPKeysAndOrdinaryKeys visitExchangeNode(ExchangeNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitSortingNode(SortingNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitAggregatingNode(AggregatingNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitProjectionNode(ProjectionNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitLimitNode(LimitNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitFilterNode(FilterNode &, JoinInfo &) override;
    FPKeysAndOrdinaryKeys visitUnionNode(UnionNode &, JoinInfo &) override;

    NameSet visitFilterExpression(const ConstASTPtr & filter, FPKeysAndOrdinaryKeys & plan_and_keys);

    auto getFinalWinners() const { return final_winners; }

    template <typename Pairs>
    void collectEliminableJoin(const Pairs & winners)
    {
        for (const auto & winner : winners)
        {
            const auto & bottom_joins = winner.second.bottom_joins;
            if (context->getSettingsRef().enable_eliminate_complicated_pk_fk_join && bottom_joins.size() > 1)
            {
                // if can't construct an entire winner with top join, we split out bottom_join which can_remove_directly.
                if (std::any_of(bottom_joins.begin(), bottom_joins.end(), [&](const auto & pair) {
                        return pair.first->getId() == winner.second.winner->getId();
                    }))
                {
                    for (const auto & bottom_join : bottom_joins)
                    {
                        if (bottom_join.second.can_remove_directly)
                        {
                            final_winners.push_back(
                                {winner.first, JoinInfo::JoinWinner{.winner = bottom_join.first, .bottom_joins = {bottom_join}}});
                        }
                    }
                }
                else
                {
                    final_winners.push_back(winner);
                }
            }
            else if (bottom_joins.size() == 1 && bottom_joins.begin()->second.can_remove_directly)
            {
                final_winners.push_back(winner);
            }
        }
    }

private:
    ContextMutablePtr context;
    NodeCTEVisitHelper<FPKeysAndOrdinaryKeys, JoinInfo> cte_helper;
    const TableColumnInfo & info;
    std::vector<std::pair<String, JoinInfo::JoinWinner>> final_winners;
};

struct JoinEliminationContext
{
    // Ordered container are used to ensure that the returned results are fixed (otherwise, although each result is valid, the output will be unstable).
    bool in_eliminating = false;

    std::map<String, std::pair<String, DataTypePtr>> current_pk_to_fk;
    std::unordered_map<String, String> current_pk_to_original_name;
    // columns which isn't pk in `pk table`, should be erase in EliminateByForeignKey::Eliminator. iter.second is original name.
    std::unordered_map<String, String> current_ordinary_columns;
    // During the elimination process, manually complete these fk headers in the following operators: agg, projection, union. Avoid not finding fk symbol when completing join on union.
    NamesAndTypes additional_fk_for_multi_child_node;

    void clear()
    {
        in_eliminating = false;
        additional_fk_for_multi_child_node.clear();
        current_pk_to_fk.clear();
        current_pk_to_original_name.clear();
    }

    JoinEliminationContext operator|(const JoinEliminationContext & other) const
    {
        JoinEliminationContext new_context = *this;

        new_context.in_eliminating = in_eliminating || other.in_eliminating;

        for (const auto & pk_to_pk : other.current_pk_to_original_name)
            new_context.current_pk_to_original_name.insert(pk_to_pk);
        for (const auto & pk_to_fk : other.current_pk_to_fk)
            new_context.current_pk_to_fk.insert(pk_to_fk);
        new_context.current_ordinary_columns.insert(other.current_ordinary_columns.begin(), other.current_ordinary_columns.end());
        new_context.additional_fk_for_multi_child_node.insert(
            new_context.additional_fk_for_multi_child_node.end(),
            other.additional_fk_for_multi_child_node.begin(),
            other.additional_fk_for_multi_child_node.end());
        return new_context;
    }

    void translate(const std::unordered_map<String, String> & identities, ContextMutablePtr context)
    {
        std::map<String, std::pair<String, DataTypePtr>> new_pk_to_fk;
        for (const auto & pair : current_pk_to_fk)
        {
            String pk_current_name = pair.first;
            String fk_current_name = pair.second.first;
            if (identities.contains(pk_current_name))
            {
                if (pk_current_name != identities.at(pk_current_name) && !new_pk_to_fk.contains(identities.at(pk_current_name)))
                {
                    // create new name for fk, if pk name is updating.
                    fk_current_name = context->getSymbolAllocator()->newSymbol(fk_current_name);
                    pk_current_name = identities.at(pk_current_name);
                }
            }
            else if (identities.contains(fk_current_name))
            {
                fk_current_name = identities.at(fk_current_name);
            }
            new_pk_to_fk.emplace(pk_current_name, std::pair<String, DataTypePtr>{fk_current_name, pair.second.second});
        }
        current_pk_to_fk = std::move(new_pk_to_fk);

        std::unordered_map<String, String> new_pk_to_original_name;
        for (const auto & pair : current_pk_to_original_name)
        {
            String pk_current_name = pair.first;
            if (identities.contains(pk_current_name))
            {
                pk_current_name = identities.at(pk_current_name);
            }
            new_pk_to_original_name.emplace(pk_current_name, pair.second);
        }
        current_pk_to_original_name = std::move(new_pk_to_original_name);

        std::unordered_map<String, String> new_ordinary_columns_in_pk_table;
        for (const auto & [column_name, original_name] : current_ordinary_columns)
        {
            if (identities.contains(column_name))
                new_ordinary_columns_in_pk_table.emplace(identities.at(column_name), original_name);
            else
                new_ordinary_columns_in_pk_table.emplace(column_name, original_name);
        }
        current_ordinary_columns = std::move(new_ordinary_columns_in_pk_table);

        NamesAndTypes new_additional_fk_for_multi_child_node;
        for (const auto & pair : additional_fk_for_multi_child_node)
        {
            if (identities.contains(pair.name))
                new_additional_fk_for_multi_child_node.emplace_back(identities.at(pair.name), pair.type);
            else
                new_additional_fk_for_multi_child_node.push_back(pair);
        }
        additional_fk_for_multi_child_node = new_additional_fk_for_multi_child_node;
    }
};

class EliminateJoinByFK::Eliminator : public PlanNodeVisitor<PlanNodePtr, JoinEliminationContext>
{
public:
    explicit Eliminator(
        ContextMutablePtr context_,
        CTEInfo & cte_info_,
        const std::pair<String, JoinInfo::JoinWinner> winner_,
        const TableColumnInfo & info_)
        : context(context_), cte_helper(cte_info_), winner(winner_)
    {
        for (const auto & pk : info_.table_columns.at(winner.first).getPrimaryKeySet())
            pk_columns_in_pk_table.emplace(pk.getCurrentName(), pk.getColumnName());

        for (const auto & key : info_.table_ordinary_columns.at(winner.first))
            original_ordinary_columns_in_pk_table.emplace(key.getColumnName(), key.getColumnName());
    }
    PlanNodePtr visitPlanNode(PlanNodeBase &, JoinEliminationContext &) override;

    PlanNodePtr visitProjectionNode(ProjectionNode &, JoinEliminationContext &) override;
    PlanNodePtr visitJoinNode(JoinNode &, JoinEliminationContext &) override;
    PlanNodePtr visitTableScanNode(TableScanNode &, JoinEliminationContext &) override;
    PlanNodePtr visitFilterNode(FilterNode &, JoinEliminationContext &) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode &, JoinEliminationContext &) override;
    PlanNodePtr visitUnionNode(UnionNode &, JoinEliminationContext &) override;
    PlanNodePtr visitCTERefNode(CTERefNode &, JoinEliminationContext &) override;

private:
    PlanNodePtr createNewJoinThenEnd(
        const String & fk_name, PlanNodePtr current_node, const NamesAndTypes & expected_header, JoinEliminationContext & c);
    PlanNodePtr createNewProjectionThenEnd(PlanNodePtr child_node, const NamesAndTypes & expected_header, JoinEliminationContext & c);

    ContextMutablePtr context;
    NodeCTEVisitHelper<PlanNodePtr, JoinEliminationContext> cte_helper;
    std::pair<String, JoinInfo::JoinWinner> winner; // pk tbl_name -> {join_number, top_join_ptr, bottom_joins_ptr}
    std::unordered_map<String, String> pk_columns_in_pk_table;
    std::unordered_map<String, String> original_ordinary_columns_in_pk_table;
};
}
