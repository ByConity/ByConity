#pragma once
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Common/filesystemHelpers.h>

namespace DB
{

#define CHECKPOINTS_DIR "checkpoints"

#define CHECKPOINT_MAX_BLOCK_SIZE 100000

#define CHECKPOINT_MAX_BLOCKS_SIZE_BYTES 10000000

inline String getCheckpointRelativePath(const MergeTreeMetaBase & storage)
{
    return joinPaths({storage.getRelativeDataPath(IStorage::StorageLocation::MAIN), CHECKPOINTS_DIR});
}

}
