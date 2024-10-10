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

#include <Storages/IStorage.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Optimizer/PredicateUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Pipe.h>
#include <QueryPlan/PlanSegmentSourceStep.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <Storages/AlterCommands.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
    extern const int PART_COLUMNS_NOT_FOUND_IN_TABLE_VERSIONS;
}


bool IStorage::isVirtualColumn(const String & column_name, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Virtual column maybe overridden by real column
    return !metadata_snapshot->getColumns().has(column_name) && getVirtuals().contains(column_name);
}

RWLockImpl::LockHolder IStorage::tryLockTimed(
    const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const std::chrono::milliseconds & acquire_timeout) const
{
    auto lock_holder = rwlock->getLock(type, query_id, acquire_timeout);
    if (!lock_holder)
    {
        const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
        throw Exception(
            type_str + " locking attempt on \"" + getStorageID().getFullTableName() + "\" has timed out! ("
                + std::to_string(acquire_timeout.count())
                + "ms) "
                  "Possible deadlock avoided. Client should retry.",
            ErrorCodes::DEADLOCK_AVOIDED);
    }
    return lock_holder;
}

TableLockHolder IStorage::lockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLockImpl::Read, query_id, acquire_timeout);

    if (is_dropped)
    {
        auto table_id = getStorageID();
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped", table_id.database_name, table_id.table_name);
    }

    return result;
}

TableLockHolder IStorage::lockForAlter(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(alter_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    return result;
}


TableExclusiveLockHolder IStorage::lockExclusively(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableExclusiveLockHolder result;
    result.alter_lock = tryLockTimed(alter_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    result.drop_lock = tryLockTimed(drop_lock, RWLockImpl::Write, query_id, acquire_timeout);

    return result;
}

Pipe IStorage::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void IStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    auto pipe = read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    if (pipe.empty())
    {
        auto header = (query_info.projection ? query_info.projection->desc->metadata : storage_snapshot->metadata)
                          ->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, context);
    }
    else
    {
        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName());
        query_plan.addStep(std::move(read_step));
    }
}

void IStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams,
    bool distributed_stages)
{
    if (distributed_stages)
    {
        //IStorage::read(query_plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
        auto header = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, context, processed_stage);
        auto read_step = std::make_unique<PlanSegmentSourceStep>(header,
                                                              getStorageID(),
                                                              query_info,
                                                              column_names,
                                                              processed_stage,
                                                              max_block_size,
                                                              num_streams,
                                                              context);
        read_step->setStepDescription(getStorageID().getNameForLogs());
        query_plan.addStep(std::move(read_step));
    }
    else
        throw Exception("Shouldn't call this read function if it is not a distributed stage query", ErrorCodes::LOGICAL_ERROR);
}

Pipe IStorage::alterPartition(
    const StorageMetadataPtr & /* metadata_snapshot */, const PartitionCommands & /* commands */, ContextPtr /* context */, const ASTPtr & /* query */)
{
    throw Exception("Partition operations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void IStorage::alter(const AlterCommands & params, ContextPtr context, TableLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadataCopy();
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name, context)->alterTable(context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void IStorage::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter())
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

void IStorage::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    throw Exception("Table engine " + getName() + " doesn't support mutations", ErrorCodes::NOT_IMPLEMENTED);
}

void IStorage::checkAlterPartitionIsPossible(
    const PartitionCommands & /*commands*/, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & /*settings*/) const
{
    throw Exception("Table engine " + getName() + " doesn't support partitioning", ErrorCodes::NOT_IMPLEMENTED);
}

StorageID IStorage::getStorageID() const
{
    std::lock_guard lock(id_mutex);
    return storage_id;
}

void IStorage::renameInMemory(const StorageID & new_table_id)
{
    /// Do not change server_vw_name here
    auto old_server_vw_name = storage_id.server_vw_name;
    std::lock_guard lock(id_mutex);
    storage_id = new_table_id;
    storage_id.server_vw_name = old_server_vw_name;
}

NamesAndTypesList IStorage::getVirtuals() const
{
    return {};
}

Names IStorage::getAllRegisteredNames() const
{
    Names result;
    auto getter = [](const auto & column) { return column.name; };
    auto storage_metadata = getInMemoryMetadataPtr();
    const NamesAndTypesList & available_columns = storage_metadata->getColumns().getAllPhysical();
    std::transform(available_columns.begin(), available_columns.end(), std::back_inserter(result), getter);
    return result;
}

NameDependencies IStorage::getDependentViewsByColumn(ContextPtr context) const
{
    NameDependencies name_deps;
    std::vector<StoragePtr> dependent_views;
    if (context->getServerType() == ServerType::cnch_server ||
        context->getServerType() == ServerType::cnch_daemon_manager)
    {
        auto start_time = context->getTimestamp();
        auto catalog_client = context->getCnchCatalog();
        if (!catalog_client)
            throw Exception("getDependentViewsByColumn to get catalog client failed", ErrorCodes::LOGICAL_ERROR);
        dependent_views = catalog_client->getAllViewsOn(*context, std::const_pointer_cast<IStorage>(shared_from_this()), start_time);
    }
    else
    {
        auto dependencies = DatabaseCatalog::instance().getDependencies(storage_id);
        for (const auto & depend_id : dependencies)
        {
            auto depend_table = DatabaseCatalog::instance().getTable(depend_id, context);
            dependent_views.emplace_back(depend_table);
        }
    }
    for (const auto & view : dependent_views)
    {
        if (view->getInMemoryMetadataPtr()->select.inner_query)
        {
            Names required_columns;
            const auto & select_query = view->getInMemoryMetadataPtr()->select.inner_query;
            if (auto * select = select_query->as<ASTSelectWithUnionQuery>())
            {
                if (select->list_of_selects->children.size() == 1)
                    required_columns = InterpreterSelectQuery(select->list_of_selects->children.at(0)->clone(), context, SelectQueryOptions{}.noModify()).getRequiredColumns();
            }
            else if (select_query->as<ASTSelectQuery>())
                required_columns = InterpreterSelectQuery(select_query->clone(), context, SelectQueryOptions{}.noModify()).getRequiredColumns();
            for (const auto & col_name : required_columns)
                name_deps[col_name].push_back(view->getTableName());
        }
    }
    return name_deps;
}

NamesAndTypesListPtr IStorage::getPartColumns(const UInt64 &columns_commit_time) const
{
    if (columns_commit_time == commit_time.toUInt64())
        return part_columns;
    auto it = previous_versions_part_columns.find(columns_commit_time);
    if (it == previous_versions_part_columns.end())
        throw Exception("Part's columns_commit_time " + toString(columns_commit_time) + " not found in table versions", ErrorCodes::PART_COLUMNS_NOT_FOUND_IN_TABLE_VERSIONS);
    return it->second;
}

UInt64 IStorage::getPartColumnsCommitTime(const NamesAndTypesList &search_part_columns) const
{
    // find the exactly the same columns first. If cannot find one, use the most recent one that could be best compitible with the data.
    UInt64 most_recend_quilified = 0;
    size_t distance = search_part_columns.size();

    auto getColumnsCommitTimeInternal = [&](const UInt64 commit_ts, const NamesAndTypesListPtr & columns_ptr)
    {
        UInt64 res = 0;
        NamesAndTypesList deleted;
        NamesAndTypesList added;

        columns_ptr->getDifference(search_part_columns, deleted, added);

        // return directly if find the mathing columns version
        if (deleted.empty() && added.empty())
            res = commit_ts;
        // data part has more columns than storage
        else if (deleted.empty() && added.size() < distance)
        {
            distance = added.size();
            most_recend_quilified = commit_ts;
        }
        return res;
    };

    if (auto ts = getColumnsCommitTimeInternal(commit_time.toUInt64(), part_columns); ts)
        return ts;

    for (auto it = previous_versions_part_columns.crbegin(); it != previous_versions_part_columns.crend(); it++)
    {
        if (auto ts = getColumnsCommitTimeInternal(it->first, it->second); ts)
            return ts;
    }
    return most_recend_quilified;
}

ASTPtr IStorage::applyFilter(ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr, PlanNodeStatisticsPtr) const
{
    // only set query.where()
    auto * select_query = query_info.getSelectQuery();

    if (!PredicateUtils::isTruePredicate(query_filter))
    {
        if (auto where = select_query->where())
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(ASTs{query_filter, where}));
        else
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, ASTPtr{query_filter});
    }

    return query_filter;
}

void IStorage::restoreDataFromBackup(BackupTaskPtr &, const DiskPtr &, const String &, ContextMutablePtr, std::optional<ASTs>)
{
    throw Exception("Table engine " + getName() + " doesn't support restoring.", ErrorCodes::NOT_IMPLEMENTED);
}

std::string PrewhereInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "PrewhereDagInfo\n";

    if (alias_actions)
    {
        ss << "alias_actions " << alias_actions->dumpDAG() << "\n";
    }

    if (prewhere_actions)
    {
        ss << "prewhere_actions " << prewhere_actions->dumpDAG() << "\n";
    }

    ss << "remove_prewhere_column " << remove_prewhere_column
       << ", need_filter " << need_filter << "\n";

    return ss.str();
}

std::string FilterDAGInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "FilterDAGInfo for column '" << column_name <<"', do_remove_column "
       << do_remove_column << "\n";
    if (actions)
    {
        ss << "actions " << actions->dumpDAG() << "\n";
    }

    return ss.str();
}

}
