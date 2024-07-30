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

#    include <list>
#    include <boost/noncopyable.hpp>
#    include "common/types.h"
#    include "Core/Names.h"
#    include "Parsers/IAST_fwd.h"

namespace Poco
{
class Logger;
}

namespace DB
{
class ASTFunction;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class HiveWhereOptimizer : private boost::noncopyable
{
public:
    HiveWhereOptimizer(
        const StorageMetadataPtr & metadata_snapshot_,
        const ASTPtr & filter_conditions);

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

    NameSet partition_key_names;
    NameSet cluster_key_names;
};

}

#endif
