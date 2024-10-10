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

#include <Transaction/TransactionCommon.h>

#include <Catalog/Catalog.h>
#include <Core/Types.h>
#include "common/logger_useful.h"
#include <Common/Exception.h>
// #include <Transaction/CnchExplicitTransaction.h>
#include <Disks/IDisk.h>
#include <Disks/DiskByteS3.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/S3ObjectMetadata.h>
#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <Interpreters/Context.h>
#include <cppkafka/cppkafka.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_CAST;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

const char * txnStatusToString(CnchTransactionStatus status)
{
    switch (status)
    {
        case CnchTransactionStatus::Running:
            return "Running";
        case CnchTransactionStatus::Finished:
            return "Finished";
        case CnchTransactionStatus::Aborted:
            return "Aborted";
        case CnchTransactionStatus::Inactive:
            return "Inactive";
        case CnchTransactionStatus::Unknown:
            return "Unknown";
    }

    throw Exception("Bad type of Transaction Status", ErrorCodes::BAD_TYPE_OF_FIELD);
}

const char * txnPriorityToString(CnchTransactionPriority priority)
{
    switch (priority)
    {
        case CnchTransactionPriority::low:
            return "Low";
        case CnchTransactionPriority::high:
            return "High";
    }

    throw Exception("Bad type of Transaction Priority", ErrorCodes::BAD_TYPE_OF_FIELD);
}

const char * txnTypeToString(CnchTransactionType type)
{
    switch (type)
    {
        case CnchTransactionType::Implicit:
            return "Implicit";
        case CnchTransactionType::Explicit:
            return "Explicit";
    }

    throw Exception("Bad type of Transaction Type", ErrorCodes::BAD_TYPE_OF_FIELD);
}

const char * txnInitiatorToString(CnchTransactionInitiator initiator)
{
    switch (initiator)
    {
        case CnchTransactionInitiator::Server:
            return "Server";

        case CnchTransactionInitiator::Worker:
            return "Worker";

        case CnchTransactionInitiator::Kafka:
            return "Kafka";

        case CnchTransactionInitiator::Merge:
            return "Merge";

        case CnchTransactionInitiator::GC:
            return "GC";

        case CnchTransactionInitiator::Txn:
            return "Txn";

        case CnchTransactionInitiator::MvRefresh:
            return "MvRefresh";

        case CnchTransactionInitiator::MergeSelect:
            return "MergeSelect";
    }

    throw Exception("Bad type of Transaction Initiator", ErrorCodes::BAD_TYPE_OF_FIELD);
}

void UndoResource::clean(Catalog::Catalog & , [[maybe_unused]]MergeTreeMetaBase * storage) const
{
    if (metadataOnly())
        return;
    DiskPtr disk;
    if (diskName().empty())
    {
        // For cnch, this storage policy should only contains one disk
        disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
    }
    else
    {
        disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getDiskByName(diskName());
    }

    if (!disk)
    {
        disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
        if (!disk || disk->getType() == DiskType::Type::Local)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can't find any remote disk in config but get `{}` disk",
                !disk ? "Empty" : disk->getName());
        }

        LOG_WARNING(log, "Disk {} not found and fallback use default disk {}", diskName(), disk->getName());
    }

    if (type() == UndoResourceType::Part || type() == UndoResourceType::DeleteBitmap || type() == UndoResourceType::StagedPart
        || type() == UndoResourceType::S3DetachDeleteBitmap || type() == UndoResourceType::S3AttachDeleteBitmap)
    {
        const auto & resource_relative_path = type() == UndoResourceType::S3AttachDeleteBitmap ? placeholders(4) : placeholders(1);
        /// For HDFS, rel_path is {table_uuid} / {part_id}.
        /// For S3, as storage->getRelativeDataPath returns "", rel_path is just {part_id}
        String rel_path = fs::path(storage->getRelativeDataPath(IStorage::StorageLocation::MAIN)) / resource_relative_path;
        if (disk->exists(rel_path))
        {
            if ((type() == UndoResourceType::Part || type() == UndoResourceType::StagedPart)
                && disk->getType() == DiskType::Type::ByteS3)
            {
                if (auto s3_disk = std::dynamic_pointer_cast<DiskByteS3>(disk); s3_disk != nullptr)
                {
                    S3PartsLazyCleaner cleaner(s3_disk->getS3Util(), s3_disk->getPath(),
                        S3ObjectMetadata::PartGeneratorID(S3ObjectMetadata::PartGeneratorID::TRANSACTION, std::to_string(txn_id)), 1);

                    cleaner.push(resource_relative_path);

                    cleaner.finalize();
                }
            }
            else
            {
                LOG_DEBUG(log, "Will remove Disk {} undo path {}" , disk->getPath(), rel_path);
                disk->removeRecursive(rel_path);
            }
        }
    }
    else if (type() == UndoResourceType::FileSystem)
    {
        const String & src_path = placeholders(0);
        const String & dst_path = placeholders(1);
        if (!disk->exists(dst_path))
        {
            LOG_TRACE(log, "Disk {} does not contain {}, nothing to move", disk->getPath(), dst_path);
        }
        else
        {
            const String & dst_table_uuid = uuid();
            LOG_TRACE(log, "Deal with dst_table {}, dst_path {}, src_path {}", dst_table_uuid, dst_path, src_path);
            if (dst_path.ends_with(dst_table_uuid) || dst_path.ends_with(dst_table_uuid + "/"))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "dst_path {} should NEVER end with table_uuid. It is a dangerous path.", dst_path);

            fs::path p(src_path);
            String parent_path;
            /// For path like xxx/yyy/ (part path), getting parent_path xxx/ needs call path.parent_path().parent_path().
            /// For path like xxx/yyy (delete bitmap path), getting parent_path xxx/ needs call path.parent_path()
            if (src_path.ends_with('/'))
                parent_path = p.parent_path().parent_path();
            else
                parent_path = p.parent_path();

            if (!disk->exists(parent_path))
            {
                LOG_WARNING(log, "src_path of {} does not exist, skip moving data and just remove dst_path of {}", parent_path, dst_path);
                disk->removeRecursive(dst_path);
            }
            /// move dst to src
            else
                disk->moveDirectory(dst_path, src_path);
        }
    }
    else if (type() == UndoResourceType::KVFSLockKey)
    {
        LOG_TRACE(log, "Nothing to clean in vfs for undo resource:" + toDebugString());
    }
    else
    {
        LOG_DEBUG(log, "Undefined clean method for undo resource: " + toDebugString());
    }
}

void UndoResource::commit(const Context & context) const
{
    auto catalog = context.getCnchCatalog();

    if (type() == UndoResourceType::S3AttachDeleteBitmap)
    {
        const String & from_tbl_uuid = placeholders(0);
        const String & former_bitmap_meta = placeholders(2);

        StoragePtr table = catalog->tryGetTableByUUID(context, from_tbl_uuid, TxnTimestamp::maxTS(), true);
        if (!table)
            return;

        auto * storage = dynamic_cast<MergeTreeMetaBase *>(table.get());
        if (!storage)
            throw Exception("Table is not of MergeTree class", ErrorCodes::BAD_CAST);

        DiskPtr disk;
        if (diskName().empty())
        {
            // For cnch, this storage policy should only contains one disk
            disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
        }
        else
        {
            disk = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getDiskByName(diskName());
        }

        /// This can happen in testing environment when disk name may change time to time
        if (!disk)
        {
            throw Exception("Disk " + diskName() + " not found. This should only happens in testing or unstable environment. If this exception is on production, there's a bug", ErrorCodes::LOGICAL_ERROR);
        }

        DataModelDeleteBitmapPtr model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
        model_ptr->ParseFromString(former_bitmap_meta);
        const auto & relative_path = DeleteBitmapMeta::deleteBitmapFileRelativePath(*model_ptr);
        String rel_path = fs::path(storage->getRelativeDataPath(IStorage::StorageLocation::MAIN)) / relative_path;
        if (disk->exists(rel_path))
        {
            LOG_DEBUG(log, "Will remove Disk {} undo path {}", disk->getPath(), rel_path);
            disk->removeRecursive(rel_path);
        }
    }
}

UndoResourceNames integrateResources(const UndoResources & resources)
{
    UndoResourceNames result;
    for (const auto & resource : resources)
    {
        if (resource.type() == UndoResourceType::Part || resource.type() == UndoResourceType::S3VolatilePart)
        {
            result.parts.insert(resource.placeholders(0));
        }
        else if (resource.type() == UndoResourceType::DeleteBitmap)
        {
            result.bitmaps.insert(resource.placeholders(0));
        }
        else if (resource.type() == UndoResourceType::StagedPart)
        {
            result.staged_parts.insert(resource.placeholders(0));
        }
        else if (resource.type() == UndoResourceType::FileSystem)
        {
            /// try to get part name from dst path
            String dst_path = resource.placeholders(1);
            if (!dst_path.empty() && dst_path.back() == '/')
                dst_path.pop_back();
            String part_name = dst_path.substr(dst_path.find_last_of('/') + 1);
            MergeTreePartInfo part_info;
            if (MergeTreePartInfo::tryParsePartName(part_name, &part_info, MERGE_TREE_CHCH_DATA_STORAGTE_VERSION)
                && part_info.mutation == static_cast<Int64>(resource.txn_id))
            {
                /// Here we need to check the part mutation match with the transaction id as well, because we also record
                /// intermediate moves. If this undo buffer is for an intermediate move, part_name can be the name of old
                /// parts (before attach).
                result.parts.insert(part_name);
            }
        }
        else if (resource.type() == UndoResourceType::KVFSLockKey)
        {
            result.kvfs_lock_keys.insert(resource.placeholders(0));
        }
        else if (resource.type() == UndoResourceType::S3AttachPart
            || resource.type() == UndoResourceType::S3DetachPart
            || resource.type() == UndoResourceType::S3DetachStagedPart
            || resource.type() == UndoResourceType::S3AttachMeta
            || resource.type() == UndoResourceType::S3DetachDeleteBitmap
            || resource.type() == UndoResourceType::S3AttachDeleteBitmap)
        {
        }
        else
            throw Exception("Unknown undo resource type " + toString(static_cast<int>(resource.type())), ErrorCodes::LOGICAL_ERROR);
    }
    return result;
}

}
