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

#pragma once

#include <Common/Logger.h>
#include <memory>
#include <Disks/IDisk.h>
#include <IO/S3Common.h>
#include <Storages/MergeTree/S3ObjectMetadata.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>

namespace DB
{
class S3PartsLazyCleaner
{
public:
    S3PartsLazyCleaner(
        const S3::S3Util & s3_util_,
        const String & data_key_prefix_,
        const std::optional<S3ObjectMetadata::PartGeneratorID> & generator_id_,
        size_t max_threads_,
        size_t batch_clean_size_ = S3::S3_DEFAULT_BATCH_CLEAN_SIZE);

    void push(const String & part_rel_key_);
    void finalize();

private:
    bool filterPartToRemove(const S3::S3Util & s3_util_, const String & key_);

    LoggerPtr logger;
    const String data_key_prefix;
    std::optional<S3ObjectMetadata::PartGeneratorID> generator_id;

    std::unique_ptr<S3::S3LazyCleaner> lazy_cleaner;
};

class MultiDiskS3PartsLazyCleaner
{
public:
    MultiDiskS3PartsLazyCleaner(
        const std::optional<S3ObjectMetadata::PartGeneratorID> & generator_id_,
        size_t max_threads_per_cleaner_,
        size_t batch_clean_size_ = S3::S3_DEFAULT_BATCH_CLEAN_SIZE)
        : max_threads_per_cleaner(max_threads_per_cleaner_), batch_clean_size(batch_clean_size_), generator_id(generator_id_)
    {
    }

    void push(const DiskPtr & disk_, const String & part_relative_key_);
    void finalize();

private:
    size_t max_threads_per_cleaner;
    size_t batch_clean_size;
    std::optional<S3ObjectMetadata::PartGeneratorID> generator_id;

    std::mutex cleaner_mu;
    std::map<String, std::pair<DiskPtr, std::unique_ptr<S3PartsLazyCleaner>>> cleaners;
};

// Reader support read multiple task by task id prefix, but writer and cleaner
// only support operation by single task.
class S3PartsAttachMeta
{
public:
    // First is part name, second is part id
    using PartMeta = std::pair<String, String>;

    class Writer
    {
    public:
        explicit Writer(S3PartsAttachMeta & meta_) : meta(meta_), next_write_idx(0) { }

        void write(const std::vector<PartMeta> & metas_);

    private:
        S3PartsAttachMeta & meta;

        size_t next_write_idx;

        std::vector<PartMeta> part_metas;
    };

    class Reader
    {
    public:
        Reader(S3PartsAttachMeta & meta_, size_t thread_num_) : meta(meta_), thread_num(thread_num_) { }

        const std::vector<PartMeta> & metas();

    private:
        S3PartsAttachMeta & meta;

        const size_t thread_num;

        std::optional<std::vector<PartMeta>> part_metas;
    };

    class Cleaner
    {
    public:
        Cleaner(S3PartsAttachMeta & meta_, bool clean_meta_, bool clean_data_, size_t thread_num_)
            : meta(meta_), clean_meta(clean_meta_), clean_data(clean_data_), clean_thread_num(thread_num_)
        {
        }

        void clean();

    private:
        S3PartsAttachMeta & meta;

        const bool clean_meta;
        const bool clean_data;
        const size_t clean_thread_num;
    };

    static String metaPrefix(const String & task_id_prefix_);
    static String metaFileKey(const String & task_id_, size_t idx_);

    S3PartsAttachMeta(
        const std::shared_ptr<Aws::S3::S3Client> & client_,
        const String & bucket_,
        const String & data_prefix_,
        const String & task_id_prefix_);

    const S3ObjectMetadata::PartGeneratorID & id() const { return generator_id; }

private:
    std::vector<String> listMetaFiles();
    std::vector<PartMeta> listPartsInMetaFile(const std::vector<String> & meta_files, size_t thread_num);

    const String data_key_prefix;
    const S3ObjectMetadata::PartGeneratorID generator_id;

    S3::S3Util s3_util;
};

}
