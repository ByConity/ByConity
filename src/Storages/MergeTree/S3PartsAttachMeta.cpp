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
#include <mutex>
#include <Disks/DiskByteS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <IO/RAReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <Common/ThreadPool.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

S3PartsLazyCleaner::S3PartsLazyCleaner(
    const S3::S3Util & s3_util_,
    const String & data_key_prefix_,
    const std::optional<S3ObjectMetadata::PartGeneratorID> & generator_id_,
    size_t max_threads_,
    size_t batch_clean_size_)
    : logger(getLogger("S3PartsLazyCleaner"))
    , data_key_prefix(data_key_prefix_)
    , generator_id(generator_id_)
    , lazy_cleaner(nullptr)
{
    lazy_cleaner = std::make_unique<S3::S3LazyCleaner>(
        s3_util_,
        [this](const S3::S3Util & s3_util, const String & key) { return filterPartToRemove(s3_util, key); },
        max_threads_,
        batch_clean_size_);
}

void S3PartsLazyCleaner::push(const String & part_rel_key_)
{
    String part_key = std::filesystem::path(data_key_prefix) / part_rel_key_ / "data";
    lazy_cleaner->push(part_key);
}

void S3PartsLazyCleaner::finalize()
{
    lazy_cleaner->finalize();
}

bool S3PartsLazyCleaner::filterPartToRemove(const S3::S3Util & s3_util_, const String & key_)
{
    if (!generator_id.has_value())
    {
        return true;
    }

    if (!s3_util_.exists(key_))
    {
        LOG_DEBUG(logger, fmt::format("Object {} not eixst, skip cleaning", key_));
        return false;
    }

    std::map<String, String> object_metadata = s3_util_.getObjectMeta(key_);

    String generator_meta;
    if (auto iter = object_metadata.find(S3ObjectMetadata::PART_GENERATOR_ID); iter != object_metadata.end())
    {
        // Verify meta and remove if necessary
        generator_meta = iter->second;

        if (generator_id.value().verify(generator_meta))
        {
            return true;
        }
    }

    LOG_INFO(logger, "Skip part clean since meta not match, part key: {}, generator metadata: {}", key_, generator_meta);
    return false;
}

void MultiDiskS3PartsLazyCleaner::push(const DiskPtr & disk_, const String & part_relative_key_)
{
    S3PartsLazyCleaner * cleaner = nullptr;
    {
        std::lock_guard<std::mutex> lock(cleaner_mu);

        auto iter = cleaners.find(disk_->getName());
        if (iter == cleaners.end())
        {
            if (std::shared_ptr<DiskByteS3> s3_disk = std::dynamic_pointer_cast<DiskByteS3>(disk_); s3_disk != nullptr)
            {
                std::unique_ptr<S3PartsLazyCleaner> disk_cleaner = std::make_unique<S3PartsLazyCleaner>(
                    s3_disk->getS3Util(), s3_disk->getPath(), generator_id, max_threads_per_cleaner, batch_clean_size);
                cleaner = disk_cleaner.get();
                cleaners.emplace(disk_->getName(), std::pair<DiskPtr, std::unique_ptr<S3PartsLazyCleaner>>(disk_, std::move(disk_cleaner)));
            }
            else
            {
                throw Exception(
                    fmt::format(
                        "Passing a non s3 disk to MultiDiskS3PartsLazyCleaner, disk {} has type {}",
                        disk_->getName(),
                        DiskType::toString(disk_->getType())),
                    ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else
        {
            cleaner = iter->second.second.get();
        }
    }

    cleaner->push(part_relative_key_);
}

void MultiDiskS3PartsLazyCleaner::finalize()
{
    ExceptionHandler except_handler;
    ThreadPool pool(cleaners.size());

    for (auto & cleaner : cleaners)
    {
        pool.scheduleOrThrowOnError(createExceptionHandledJob([&cleaner]() { cleaner.second.second->finalize(); }, except_handler));
    }

    pool.wait();
    except_handler.throwIfException();
}

void S3PartsAttachMeta::Writer::write(const std::vector<PartMeta> & metas_)
{
    String meta_file_key = meta.data_key_prefix + metaFileKey(meta.generator_id.id, next_write_idx++);

    {
        WriteBufferFromS3 writer(meta.s3_util.getClient(), meta.s3_util.getBucket(), meta_file_key);
        writeVectorBinary(metas_, writer);
        writer.finalize();
    }
}

const std::vector<S3PartsAttachMeta::PartMeta> & S3PartsAttachMeta::Reader::metas()
{
    if (!part_metas.has_value())
    {
        std::vector<String> meta_files = meta.listMetaFiles();
        part_metas = meta.listPartsInMetaFile(meta_files, thread_num);
    }
    return part_metas.value();
}

void S3PartsAttachMeta::Cleaner::clean()
{
    if (!clean_meta && !clean_data)
    {
        return;
    }

    // List all meta files and part files
    std::vector<String> meta_files = meta.listMetaFiles();

    // Clean data objects generated by this task
    if (clean_data)
    {
        std::vector<PartMeta> part_metas = meta.listPartsInMetaFile(meta_files, clean_thread_num);
        S3PartsLazyCleaner cleaner(meta.s3_util, meta.data_key_prefix, meta.generator_id, clean_thread_num);
        for (const PartMeta & part_meta : part_metas)
        {
            cleaner.push(part_meta.second + '/');
        }
        cleaner.finalize();
    }

    // Clean meta files
    if (clean_meta)
    {
        meta.s3_util.deleteObjectsInBatch(meta_files);
    }
}

S3PartsAttachMeta::S3PartsAttachMeta(
    const std::shared_ptr<Aws::S3::S3Client> & client_, const String & bucket_, const String & data_prefix_, const String & task_id_prefix_)
    : data_key_prefix(data_prefix_), generator_id(S3ObjectMetadata::PartGeneratorID::PART_WRITER, task_id_prefix_), s3_util(client_, bucket_)
{
}

String S3PartsAttachMeta::metaPrefix(const String & task_id_prefix_)
{
    return fmt::format("attach_meta/{}", task_id_prefix_);
}

String S3PartsAttachMeta::metaFileKey(const String & task_id_, size_t idx_)
{
    return fmt::format("attach_meta/{}/{}.am", task_id_, idx_);
}

std::vector<String> S3PartsAttachMeta::listMetaFiles()
{
    std::vector<String> all_keys;
    String meta_prefix = data_key_prefix + metaPrefix(generator_id.id);

    S3::S3Util::S3ListResult result;

    do
    {
        result = s3_util.listObjectsWithPrefix(meta_prefix, result.token);
        const auto & keys = result.object_names;
        all_keys.reserve(all_keys.size() + keys.size());
        std::for_each(keys.begin(), keys.end(), [&all_keys](const String & key) {
            if (endsWith(key, ".am"))
            {
                all_keys.push_back(key);
            }
        });
    } while (result.has_more);

    return all_keys;
}

std::vector<S3PartsAttachMeta::PartMeta> S3PartsAttachMeta::listPartsInMetaFile(const std::vector<String> & meta_files_, size_t thread_num_)
{
    std::mutex mu;
    std::vector<PartMeta> part_metas;

    ExceptionHandler handler;
    ThreadPool pool(std::min(thread_num_, meta_files_.size()));
    for (const String & file : meta_files_)
    {
        pool.scheduleOrThrowOnError(createExceptionHandledJob(
            [this, file, &mu, &part_metas]() {
                RAReadBufferFromS3 reader(s3_util.getClient(), s3_util.getBucket(), file);
                std::vector<PartMeta> partial_metas;
                readVectorBinary(partial_metas, reader);

                std::lock_guard lock(mu);
                part_metas.reserve(part_metas.size() + partial_metas.size());
                part_metas.insert(part_metas.end(), partial_metas.begin(), partial_metas.end());
            },
            handler));
    }
    pool.wait();
    handler.throwIfException();

    return part_metas;
}

}
