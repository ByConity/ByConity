#include <Transaction/TransactionCommon.h>

#include <Catalog/Catalog.h>
#include <Core/Types.h>
#include "common/logger_useful.h"
#include <Common/Exception.h>
// #include <Transaction/CnchExplicitTransaction.h>
#include "Disks/IDisk.h"
#include <MergeTreeCommon/MergeTreeMetaBase.h>
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
    }

    throw Exception("Bad type of Transaction Initiator", ErrorCodes::BAD_TYPE_OF_FIELD);
}

void UndoResource::clean(CatalogService::Catalog & , [[maybe_unused]]MergeTreeMetaBase * storage) const
{
    if (metadataOnly())
        return;

    DiskPtr disk;
    ///FIXME: if storage selector is available @guanzhe.andy
    // if (diskName().empty())
    //     disk = storage->getStorageSelector().getDefaultHDFSDisk();
    // else 
    //     disk = storage->getStorageSelector().getStoragePolicy()->getDiskByName(diskName()); 

    /// This can happen in testing environment when disk name may change time to time
    if (!disk)
    {
        throw Exception("Disk " + diskName() + " not found. This should only happens in testing or unstable environment. If this exception is on production, there's a bug", ErrorCodes::LOGICAL_ERROR);
    }
    
    if (type() == UndoResourceType::Part || type() == UndoResourceType::DeleteBitmap || type() == UndoResourceType::StagedPart)
    {
        ///FIXME: if storage selector is available @guanzhe.andy
        // const auto & relative_path = placeholders(1);
        // String rel_path = storage->getStorageSelector().tableRelativePathOnDisk(disk) + relative_path;
        String rel_path = "";
        if (disk->exists(rel_path)) 
        {
            LOG_DEBUG(log, "Will remove undo path {}", disk->getPath() + rel_path);
            disk->removeRecursive(rel_path);
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
            /// move dst to src
            disk->moveDirectory(dst_path, src_path);
        }
    }
    else
    {
        LOG_DEBUG(log, "Undefined clean method.");
    }
}

UndoResourceNames integrateResources(const UndoResources & resources)
{
    UndoResourceNames result;
    for (auto & resource : resources)
    {
        if (resource.type() == UndoResourceType::Part)
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
            if (dst_path.size() > 0 && dst_path.back() == '/')
                dst_path.pop_back();
            String part_name = dst_path.substr(dst_path.find_last_of('/') + 1);
            if (MergeTreePartInfo::tryParsePartName(part_name, nullptr, MERGE_TREE_CHCH_DATA_STORAGTE_VERSION))
            {
                result.parts.insert(part_name);
            }
        }
        else
            throw Exception("Unknown undo resource type " + toString(static_cast<int>(resource.type())), ErrorCodes::LOGICAL_ERROR);
    }
    return result;
}

}
