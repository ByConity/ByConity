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

#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace
{
    constexpr UInt8 FORMAT_VERSION = 2;
    constexpr UInt8 MIN_VERSION_FOR_QUERY_ID = 2;
}

namespace DB
{

void CnchMergeTreeMutationEntry::writeText(WriteBuffer & out) const
{
    out << "format version: " << FORMAT_VERSION << "\n"
        << "query_id: " << query_id << "\n"
        << "txn_id: " << txn_id.toUInt64() << "\n"
        << "commit_ts: " << commit_time.toUInt64() << "\n"
        << "storage_commit_ts: " << columns_commit_time.toUInt64() << "\n";

    out << "commands: ";
    commands.writeText(out);
    out << "\n";
}

void CnchMergeTreeMutationEntry::readText(ReadBuffer & in)
{
    UInt8 version;
    UInt64 i_txn_id, i_commit_ts, i_columns_commit_ts;

    in >> "format version: " >> version >> "\n";
    if (version >= MIN_VERSION_FOR_QUERY_ID)
        in >> "query_id: " >> query_id >> "\n";

    in >> "txn_id: " >> i_txn_id >> "\n"
       >> "commit_ts: " >> i_commit_ts >> "\n"
       >> "storage_commit_ts: " >> i_columns_commit_ts >> "\n";

    txn_id = i_txn_id;
    commit_time = i_commit_ts;
    columns_commit_time = i_columns_commit_ts;
    in >> "commands: ";
    commands.readText(in);
    in >> "\n";
}

String CnchMergeTreeMutationEntry::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

CnchMergeTreeMutationEntry CnchMergeTreeMutationEntry::parse(const String & str)
{
    CnchMergeTreeMutationEntry res;

    ReadBufferFromString in(str);
    res.readText(in);
    assertEOF(in);

    return res;
}

bool CnchMergeTreeMutationEntry::isReclusterMutation() const
{
    return commands.size() == 1 && commands[0].type==MutationCommand::Type::RECLUSTER;
}

bool CnchMergeTreeMutationEntry::isModifyClusterBy() const
{
    return commands.size() == 1 && commands[0].type==MutationCommand::Type::MODIFY_CLUSTER_BY;
}

void CnchMergeTreeMutationEntry::setPartitionIDs(const Strings & _partition_ids)
{
    partition_ids.emplace(_partition_ids);
}

}
