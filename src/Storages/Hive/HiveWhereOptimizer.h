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


#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Core/Names.h"
#include "Parsers/IAST_fwd.h"
#include <boost/noncopyable.hpp>
#include "common/types.h"
#include <list>

namespace Poco { class Logger; }

namespace DB
{
class ASTFunction;
struct SelectQueryInfo;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class HiveWhereOptimizer : private boost::noncopyable
{
public:
    HiveWhereOptimizer(
        const StorageMetadataPtr & metadata_snapshot_,
        const SelectQueryInfo & query_info_,
        Poco::Logger * log_);

    using Conditions = ASTs;

    /// results
    ASTPtr partition_key_conds;
    ASTPtr cluster_key_conds;

private:
    static ASTPtr reconstruct(const Conditions & conditions);

    struct Data 
    {
        Conditions partiton_key_conditions;
        Conditions cluster_key_conditions;
    };

    void extractKeyConditions(Data & res, const ASTPtr & node);
    void extractKeyConditions(Data & res, const ASTPtr & node, const ASTFunction & func);

    bool isPartitionKey(const String & column_name) const;
    bool isClusterKey(const String & column_name) const;

<<<<<<< HEAD
    /// Transform conjunctions chain in WHERE expression to Conditions list.
    Conditions implicitAnalyze(const ASTPtr & expression) const;

    /// Transform Conditions list to WHERE or PREWHERE expression.
    ASTPtr reconstruct(const Conditions & conditions) const;

    bool isValidPartitionColumn(const IAST * condition) const;

    bool isSubsetOfTableColumns(const NameSet & identifiers) const;

    // bool hasColumnOfTableColumns(const HiveWhereOptimizer::Conditions & conditions) const;
    // bool hasColumnOfTableColumns() const;
    // bool isInTableColumns(const ASTPtr & node) const;

    bool getUsefullFilter(String & filter);
    bool convertImplicitWhereToUsefullFilter(String & filter);
    bool convertWhereToUsefullFilter(ASTs & conditions, String & filter);

private:
    using StringSet = std::unordered_set<String>;

    StringSet table_columns;
    ASTPtr partition_expr_ast;
    // const Context & context;
    ASTSelectQuery & select;
    const StoragePtr & storage;
=======
    NameSet partition_key_names;
    NameSet cluster_key_names;
    Poco::Logger * log;
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')
};

}

#endif
