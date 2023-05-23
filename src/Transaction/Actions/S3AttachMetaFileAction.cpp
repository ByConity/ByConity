/*
 * Copyright 2023 Bytedance Ltd. and/or its affiliates.
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

#include <memory>
#include <Disks/DiskByteS3.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <Transaction/Actions/S3AttachMetaFileAction.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
void S3AttachMetaFileAction::executeV1(TxnTimestamp)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void S3AttachMetaFileAction::executeV2()
{
}

void S3AttachMetaFileAction::abort()
{
}

void S3AttachMetaFileAction::postCommit(TxnTimestamp)
{
    // Clean meta files
    String meta_prefix = std::filesystem::path(disk->getPath()) / S3PartsAttachMeta::metaPrefix(task_id);

    S3::S3Util s3_util(disk->getS3Client(), disk->getS3Bucket());
    s3_util.deleteObjectsWithPrefix(meta_prefix, [](const S3::S3Util &, const String & key) { return endsWith(key, ".am"); });
}

void S3AttachMetaFileAction::commitByUndoBuffer(const Context & ctx, const UndoResources & resources)
{
    for (const auto & resource : resources)
    {
        if (resource.type() != UndoResourceType::S3AttachMeta)
        {
            continue;
        }
        DiskPtr disk = ctx.getDisk(resource.diskName());
        String task_id = resource.placeholders(0);
        String meta_prefix = std::filesystem::path(disk->getPath()) / S3PartsAttachMeta::metaPrefix(task_id);

        if (std::shared_ptr<DiskByteS3> disk_s3 = std::dynamic_pointer_cast<DiskByteS3>(disk); disk_s3 != nullptr)
        {
            S3::S3Util util(disk_s3->getS3Client(), disk_s3->getS3Bucket());
            util.deleteObjectsWithPrefix(meta_prefix, [](const S3::S3Util &, const String & key) { return endsWith(key, ".am"); });
        }
        else
        {
            throw Exception(
                fmt::format("Expected s3 disk from undo resource, got {}", DiskType::toString(disk->getType())), ErrorCodes::LOGICAL_ERROR);
        }
    }
}

}
