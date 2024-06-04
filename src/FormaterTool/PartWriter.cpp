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

#include <filesystem>
#include <sstream>
#include <Core/UUID.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Disks/DiskByteS3.h>
#include <Disks/HDFS/DiskByteHDFS.h>
#include <FormaterTool/PartWriter.h>
#include <IO/SnappyReadBuffer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartToolKit.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageMergeTree.h>
#include <Transaction/TransactionCommon.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Poco/UUIDGenerator.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int DIRECTORY_ALREADY_EXISTS;
}

PartWriter::PartWriter(const ASTPtr & query_ptr_, ContextMutablePtr context_) : PartToolkitBase(query_ptr_, context_)
{
    const ASTPartToolKit & pw_query = query_ptr->as<ASTPartToolKit &>();
    if (pw_query.type != PartToolType::WRITER)
        throw Exception("Wrong input query.", ErrorCodes::INCORRECT_QUERY);

    /// Init as a S3 clean task.
    if (pw_query.s3_clean_task_info)
    {
        clean_task = pw_query.s3_clean_task_info;

        applySettings();
        return;
    }

    source_path = pw_query.source_path->as<ASTLiteral &>().value.safeGet<String>();
    dest_path = pw_query.target_path->as<ASTLiteral &>().value.safeGet<String>();
    data_format = getIdentifierName(pw_query.data_format);

    if (source_path.empty() || dest_path.empty())
        throw Exception("Source path and target path cannot be empty.", ErrorCodes::LOGICAL_ERROR);

    Poco::UUIDGenerator & generator = Poco::UUIDGenerator::defaultGenerator();
    /// use `part-writer` as the parent folder to organize tmp files.
    working_path = Poco::Path::current() + "part-writer/" + generator.createRandom().toString() + "/";

    if (fs::exists(working_path))
    {
        LOG_INFO(log, "Working path {} already exists. Will remove it.", working_path);
        fs::remove_all(working_path);
    }

    LOG_INFO(log, "Creating new working directory {}.", working_path);
    fs::create_directories(working_path);
    fs::create_directory(working_path + "disks/");
    fs::create_directory(working_path + PT_RELATIVE_LOCAL_PATH);

    getContext()->setPath(working_path);

    applySettings();

    // Valid dest_path for HDFS/CFS.
    if (s3_output_config == nullptr)
    {
        Poco::URI uri(dest_path);
        if (!isHdfsOrCfsScheme(uri.getScheme()))
            throw Exception(
                "Target path must be a HDFS or CFS directory, otherwise, a s3_output_config path must be provided.",
                ErrorCodes::LOGICAL_ERROR);

        dest_path = uri.getPath();
        if (!endsWith(dest_path, "/"))
            dest_path.append("/");
    }

    if (s3_input_config)
    {
        // Overwrite S3 source_path as:
        // `S3://<bucket>/<root_path>/<data_file>`
        source_path = "s3://" + s3_input_config->bucket + "/" + s3_input_config->root_prefix + "/" + source_path;
    }
}

PartWriter::~PartWriter()
{
    if (fs::exists(working_path))
        fs::remove_all(working_path);
}

void PartWriter::execute()
{
    /// Execute S3 clean task.
    if (clean_task)
    {
        clean();
        return;
    }

    LOG_INFO(log, "PartWriter start to dump part.");
    const Settings & settings = getContext()->getSettingsRef();
    StoragePtr table = getTable();
    StorageCloudMergeTree * storage = static_cast<StorageCloudMergeTree *>(table.get());
    String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());

    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    Block sample_block = metadata_snapshot->getSampleBlock();

    /// set current transaction
    TransactionRecord record;
    record.read_only = true;
    record.setID(TxnTimestamp{1});
    getContext()->setCurrentTransaction(std::make_shared<CnchServerTransaction>(getContext(), record));

    /// prepare remote disk
    HDFSConnectionParams params = getContext()->getHdfsConnectionParams();
    LOG_INFO(log, "PartWriter hdfs params = {}", params.toString());

    std::shared_ptr<IDisk> remote_disk;
    std::unique_ptr<S3PartsAttachMeta::Writer> s3_attach_meta_writer;
    std::unique_ptr<S3PartsAttachMeta> s3_attach_meta;
    if (s3_output_config)
    {
        /// S3 remote disk.
        std::shared_ptr<Aws::S3::S3Client> client = s3_output_config->create();
        remote_disk = std::make_shared<DiskByteS3>("s3_disk", s3_output_config->root_prefix, s3_output_config->bucket, client,
            s3_output_config->min_upload_part_size, s3_output_config->max_single_part_upload_size);
        s3_attach_meta = std::make_unique<S3PartsAttachMeta>(
            client, s3_output_config->bucket, std::filesystem::path(s3_output_config->root_prefix) / "", dest_path);

        s3_attach_meta_writer = std::make_unique<S3PartsAttachMeta::Writer>(*s3_attach_meta);

        /// relative_data_path must be empty so that
        /// parts will be dumped exactly to `<Prefix>/<Part-UUID>/data`.
        storage->setRelativeDataPath(IStorage::StorageLocation::MAIN, "");
    }
    else
    {
        /// HDFS remote disk.
        remote_disk = std::make_shared<DiskByteHDFS>("hdfs", dest_path, params);

        if (remote_disk->exists(uuid))
        {
            LOG_WARNING(log, "Remote path {} already exists. Try to remove it.", dest_path + uuid);
            remote_disk->removeRecursive(uuid);
        }
        LOG_DEBUG(log, "Creating remote storage path {} for table.", dest_path + uuid);
        remote_disk->createDirectory(uuid);
    }
    S3ObjectMetadata::PartGeneratorID part_generator_id(
        S3ObjectMetadata::PartGeneratorID::PART_WRITER, UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()));
    MergeTreeCNCHDataDumper cnch_dumper(*storage, s3_output_config ? s3_attach_meta->id() : part_generator_id);
    /// prepare outputstream
    BlockOutputStreamPtr out = table->write(query_ptr, metadata_snapshot, getContext());
    CloudMergeTreeBlockOutputStream * cloud_stream = static_cast<CloudMergeTreeBlockOutputStream *>(out.get());
    bool is_enable_squash = settings.input_format_parquet_max_block_size;

    LOG_DEBUG(log, "is_enable_squash = {}, min_insert_block_size_rows = {}, min_insert_block_size_bytes = {}",
        is_enable_squash, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);

    auto input_stream = InterpreterInsertQuery::buildInputStreamFromSource(getContext(), metadata_snapshot->getColumns(), sample_block, settings, source_path, data_format, is_enable_squash);

    input_stream->readPrefix();

    while (true)
    {
        const auto block = input_stream->read();
        if (!block)
            break;
        auto parts = cloud_stream->convertBlockIntoDataParts(block, true);
        LOG_DEBUG(log, "Dumping {} parts to remote storage.", parts.size());
        if (s3_attach_meta_writer)
        {
            /// Prepare s3_task_meta.
            std::vector<std::pair<String, String>> parts_meta;
            for (const auto & temp_part : parts)
            {
                temp_part->uuid = UUIDHelpers::generateV4();
                auto relative_path = UUIDHelpers::UUIDToString(temp_part->uuid);
                parts_meta.push_back({temp_part->get_name(), relative_path});
            }
            /// Write s3_task_meta first.
            s3_attach_meta_writer->write(parts_meta);
        }
        for (const auto & temp_part : parts)
        {
            cnch_dumper.dumpTempPart(temp_part, remote_disk);
            LOG_DEBUG(log, "Dumped part {}", temp_part->name);
        }
    }

    input_stream->readSuffix();

    LOG_INFO(log, "Finish write data parts into local file system.");
}

void PartWriter::clean()
{
    auto iter = user_settings.find("s3_config");
    if (iter == user_settings.end())
    {
        throw DB::Exception("Didn't specific s3_config when clean s3's task", DB::ErrorCodes::BAD_ARGUMENTS);
    }

    DB::S3::S3Config cfg(iter->second.get<DB::String>());
    DB::S3PartsAttachMeta parts_attach_meta(cfg.create(), cfg.bucket, cfg.root_prefix + "/", clean_task->task_id);
    auto task_type = clean_task->type;
    bool clean_meta = task_type == DB::S3CleanTaskInfo::META || task_type == DB::S3CleanTaskInfo::ALL;
    bool clean_data = task_type == DB::S3CleanTaskInfo::DATA || task_type == DB::S3CleanTaskInfo::ALL;
    LOG_DEBUG(log, "Clean meta: {}, clean data: {}", clean_meta, clean_data);
    constexpr const char * s3_clean_concurrency_setting = "s3_clean_concurrency";
    UInt64 s3_clean_concurrency
        = user_settings.count(s3_clean_concurrency_setting) ? user_settings[s3_clean_concurrency_setting].safeGet<int>() : 16;
    s3_clean_concurrency = std::max(s3_clean_concurrency, 16ul);

    DB::S3PartsAttachMeta::Cleaner cleaner(parts_attach_meta, clean_meta, clean_data, s3_clean_concurrency);
    cleaner.clean();
}
}
