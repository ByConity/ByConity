/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <boost/noncopyable.hpp>
#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <set>
#include <unordered_map>


namespace Poco { class Logger; }

namespace DB
{

class ASTSelectQuery;
class ASTFunction;
class MergeTreeData;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Push down where partition predicate to query info partition_filter
void optimizePartitionPredicate(ASTPtr & query, StoragePtr storage, SelectQueryInfo & query_info, ContextPtr context);

enum class MaterializeStrategy : Int32
{
    PREWHERE = 0,
    LATE_MATERIALIZE = 1,
};

/** Identifies WHERE expressions that can be placed in PREWHERE by calculating respective
 *  sizes of columns used in particular expression and identifying "good" conditions of
 *  form "column_name = constant", where "constant" is outside some `threshold` specified in advance.
 *
 *  If there are "good" conditions present in WHERE, the one with minimal summary column size is transferred to PREWHERE.
 *  Otherwise any condition with minimal summary column size can be transferred to PREWHERE.
 *  If column sizes are unknown (in compact parts), the number of columns, participating in condition is used instead.
 */
class MergeTreeWhereOptimizer : private boost::noncopyable
{
public:
    MergeTreeWhereOptimizer(
        SelectQueryInfo & query_info_,
        ContextPtr context_,
        std::unordered_map<std::string, UInt64> column_sizes_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & queried_columns_,
        Poco::Logger * log_,
        MaterializeStrategy materialize_strategy_ = MaterializeStrategy::PREWHERE);

    std::vector<ASTPtr> && getAtomicPredicatesExpressions();

private:
    void optimize(ASTSelectQuery & select) const;

    struct Condition
    {
        ASTPtr node;
        UInt64 columns_size = 0;
        NameSet identifiers;

        /// Can condition be moved to prewhere?
        bool viable = false;

        /// Does the condition presumably have good selectivity?
        bool good = false;

        auto tuple() const
        {
            return std::make_tuple(!viable, !good, columns_size, identifiers.size());
        }

        /// Is condition a better candidate for moving to PREWHERE?
        bool operator < (const Condition & rhs) const
        {
            return tuple() < rhs.tuple();
        }
    };

    using Conditions = std::list<Condition>;

    /// Move predicates to prewhere
    void optimizePrewhere(Conditions & where_conditions, ASTSelectQuery & select) const;

    /// Support for implementation of pipelined early materialization as described in
    /// https://15721.courses.cs.cmu.edu/spring2020/papers/13-execution/shrinivas-icde2013.pdf
    void optimizeLateMaterialize(Conditions where_conditions, ASTSelectQuery & select) const;

    void analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final) const;

    /// Transform conjunctions chain in WHERE expression to Conditions list.
    Conditions analyze(const ASTPtr & expression, bool is_final) const;

    /// Transform Conditions list to WHERE or PREWHERE expression.
    static ASTPtr reconstruct(const Conditions & conditions);

    void optimizeConjunction(ASTSelectQuery & select, ASTFunction * const fun) const;

    void optimizeArbitrary(ASTSelectQuery & select) const;

    UInt64 getIdentifiersColumnSize(const NameSet & identifiers) const;

    bool containsArraySetCheck(const ASTPtr & condition) const;

    bool isArraySetCheck(const ASTPtr & condition, bool is_not = false) const;

    bool hasPrimaryKeyAtoms(const ASTPtr & ast) const;

    bool isPrimaryKeyAtom(const ASTPtr & ast) const;

    bool isSortingKey(const String & column_name) const;

    bool isConstant(const ASTPtr & expr) const;

    bool isSubsetOfTableColumns(const NameSet & identifiers) const;

    bool isValidPartitionColumn(const IAST * condition) const;

    bool isSubsetOfPartitionKeyExpressions(const ASTPtr & node) const;

    bool defaultExpressionDependOnOtherColumns(const String & column_name) const;

    void evaluateConditionsForEMP(Conditions & conditions, const NameSet & prohibited) const;


    /** ARRAY JOIN'ed columns as well as arrayJoin() result cannot be used in PREWHERE, therefore expressions
      *    containing said columns should not be moved to PREWHERE at all.
      *    We assume all AS aliases have been expanded prior to using this class
      *
      * Also, disallow moving expressions with GLOBAL [NOT] IN.
      */
    bool cannotBeMoved(const ASTPtr & ptr, bool is_final) const;

    void determineArrayJoinedNames(ASTSelectQuery & select);

    using StringSet = std::unordered_set<std::string>;

    String first_primary_key_column;
    const StringSet table_columns;
    const Names queried_columns;
    const NameSet sorting_key_names;
    const Block block_with_constants;
    Poco::Logger * log;
    std::unordered_map<std::string, UInt64> column_sizes;
    UInt64 total_size_of_queried_columns = 0;
    NameSet array_joined_names;
    const StorageMetadataPtr & metadata_snapshot;
    bool enable_ab_index_optimization;
    bool enable_implicit_column_prewhere_push;

    /// Late materialize
    MaterializeStrategy materialize_strategy;
    bool aggresive_pushdown = false;
    Names partition_columns;
    mutable std::vector<ASTPtr> atomic_predicates_expr;
};


}
