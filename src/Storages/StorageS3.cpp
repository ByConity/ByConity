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

#include <Common/config.h>
#include "Parsers/ASTCreateQuery.h"

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <DataStreams/narrowBlockInputStreams.h>

#include <Processors/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/ReadSchemaUtils.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/DeleteObjectsRequest.h>

#include <Common/parseGlobs.h>
#include <Common/quoteString.h>
#include <re2/re2.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Pipe.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>
#include <Storages/Cache/SchemaCache.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
}

namespace CurrentMetrics
{
    extern const Metric StorageS3Threads;
    extern const Metric StorageS3ThreadsActive;
    extern const Metric StorageS3ThreadsScheduled;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int S3_ERROR;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_DETECT_FORMAT;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

// static void addPathToVirtualColumns(Block & block, const String & path, size_t idx)
// {
//     if (block.has("_path"))
//         block.getByName("_path").column->assumeMutableRef().insert(path);

//     if (block.has("_file"))
//     {
//         auto pos = path.find_last_of('/');
//         assert(pos != std::string::npos);

//         auto file = path.substr(pos + 1);
//         block.getByName("_file").column->assumeMutableRef().insert(file);
//     }

//     block.getByName("_idx").column->assumeMutableRef().insert(idx);
// }


class StorageS3Source::KeysIterator::Impl : WithContext
{
public:
    explicit Impl(
        const Aws::S3::S3Client & client_,
        const std::string & version_id_,
        const std::vector<String> & keys_,
        const String & bucket_,
        const S3Settings::RequestSettings & request_settings_,
        KeysWithInfo * read_keys_)
        : keys(keys_), version_id(version_id_), bucket(bucket_), request_settings(request_settings_)
    {
        client = std::unique_ptr<Aws::S3::S3Client>(new Aws::S3::S3Client(client_));

        if (read_keys_)
        {
            for (const auto & key : keys)
                read_keys_->push_back(std::make_shared<KeyWithInfo>(key));
        }
    }

    KeyWithInfoPtr next(size_t)
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return {};
        auto key = keys[current_index];
        std::optional<S3::ObjectInfo> info;
        return std::make_shared<KeyWithInfo>(key, info);
    }

    size_t objectsCount() const
    {
        return keys.size();
    }

private:
    Strings keys;
    std::atomic_size_t index = 0;
    std::unique_ptr<Aws::S3::S3Client> client;
    String version_id;
    String bucket;
    S3Settings::RequestSettings request_settings;
};

StorageS3Source::KeysIterator::KeysIterator(
    const Aws::S3::S3Client & client_,
    const std::string & version_id_,
    const std::vector<String> & keys_,
    const String & bucket_,
    const S3Settings::RequestSettings & request_settings_,
    KeysWithInfo * read_keys)
    : pimpl(std::make_shared<StorageS3Source::KeysIterator::Impl>(client_, version_id_, keys_, bucket_, request_settings_, read_keys))
{
}

StorageS3Source::KeyWithInfoPtr StorageS3Source::KeysIterator::next(size_t idx)
{
    return pimpl->next(idx);
}

size_t StorageS3Source::KeysIterator::estimatedKeysCount()
{
    return pimpl->objectsCount();
}
class StorageS3Source::DisclosedGlobIterator::Impl : WithContext
{
public:
    Impl(
        const Aws::S3::S3Client & client_,
        const S3::URI & globbed_uri_,
        const NamesAndTypesList & virtual_columns_,
        ContextPtr context_,
        KeysWithInfo * read_keys_,
        const S3Settings::RequestSettings & request_settings_)
        : WithContext(context_)
        , globbed_uri(globbed_uri_)
        , virtual_columns(virtual_columns_)
        , read_keys(read_keys_)
        , request_settings(request_settings_)
        , list_objects_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, 1)
        , list_objects_scheduler(threadPoolCallbackRunner<ListObjectsOutcome>(list_objects_pool, "ListObjects"))
    {
        client = std::unique_ptr<Aws::S3::S3Client>(new Aws::S3::S3Client(client_));
    
        if (globbed_uri.bucket.find_first_of("*?{") != globbed_uri.bucket.npos)
            throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "Expression can not have wildcards inside bucket name");

        expanded_keys = expandSelectionGlob(globbed_uri.key);
        expanded_keys_iter = expanded_keys.begin();

        fillBufferForKey(*expanded_keys_iter);
        expanded_keys_iter++;
    }

    KeyWithInfoPtr next(size_t)
    {
        std::lock_guard lock(mutex);
        return nextAssumeLocked();
    }

    size_t objectsCount()
    {
        return buffer.size();
    }

    ~Impl()
    {
        list_objects_pool.wait();
    }

private:
    using ListObjectsOutcome = Aws::S3::Model::ListObjectsV2Outcome;

    void fillBufferForKey(const std::string & uri_key)
    {
        is_finished_for_key = false;
        const String key_prefix = uri_key.substr(0, uri_key.find_first_of("*?{"));

        /// We don't have to list bucket, because there is no asterisks.
        if (key_prefix.size() == uri_key.size())
        {
            buffer.clear();
            buffer.emplace_back(std::make_shared<KeyWithInfo>(uri_key, std::nullopt));
            buffer_iter = buffer.begin();
            if (read_keys)
                read_keys->insert(read_keys->end(), buffer.begin(), buffer.end());
            is_finished_for_key = true;
            return;
        }

        request = {};
        request.SetBucket(globbed_uri.bucket);
        request.SetPrefix(key_prefix);
        request.SetMaxKeys(static_cast<int>(request_settings.max_list_nums));

        outcome_future = listObjectsAsync();

        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(uri_key));
        if (!matcher->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                            "Cannot compile regex from glob ({}): {}", uri_key, matcher->error());

        recursive = globbed_uri.key == "/**";

        // filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
        fillInternalBufferAssumeLocked();
    }

    KeyWithInfoPtr nextAssumeLocked()
    {
        do
        {
            if (buffer_iter != buffer.end())
            {
                auto answer = *buffer_iter;
                ++buffer_iter;

                /// If url doesn't contain globs, we didn't list s3 bucket and didn't get object info for the key.
                /// So we get object info lazily here on 'next()' request.
                if (!answer->info)
                {
                    try
                    {
                        answer->info = S3::getObjectInfo(*client, globbed_uri.bucket, answer->key, {}, request_settings);
                    }
                    catch (...)
                    {
                        /// if no such file AND there was no `{}` glob -- this is an exception
                        /// otherwise ignore it, this is acceptable
                        if (expanded_keys.size() == 1)
                            throw;
                        continue;
                    }
                    // if (file_progress_callback)
                    //     file_progress_callback(FileProgress(0, answer->info->size));
                }

                return answer;
            }

            if (is_finished_for_key)
            {
                if (expanded_keys_iter != expanded_keys.end())
                {
                    fillBufferForKey(*expanded_keys_iter);
                    expanded_keys_iter++;
                    continue;
                }
                else
                    return {};
            }

            try
            {
                fillInternalBufferAssumeLocked();
            }
            catch (...)
            {
                /// In case of exception thrown while listing new batch of files
                /// iterator may be partially initialized and its further using may lead to UB.
                /// Iterator is used by several processors from several threads and
                /// it may take some time for threads to stop processors and they
                /// may still use this iterator after exception is thrown.
                /// To avoid this UB, reset the buffer and return defaults for further calls.
                is_finished_for_key = true;
                buffer.clear();
                buffer_iter = buffer.begin();
                throw;
            }
        } while (true);
    }

    void fillInternalBufferAssumeLocked()
    {
        buffer.clear();

        assert(outcome_future.valid());
        auto outcome = outcome_future.get();

        if (!outcome.IsSuccess())
        {
            throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                            quoteString(request.GetBucket()), quoteString(request.GetPrefix()),
                            backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
        }

        const auto & result_batch = outcome.GetResult().GetContents();

        /// It returns false when all objects were returned
        is_finished_for_key = !outcome.GetResult().GetIsTruncated();

        if (!is_finished_for_key)
        {
            /// Even if task is finished the thread may be not freed in pool.
            /// So wait until it will be freed before scheduling a new task.
            list_objects_pool.wait();
            outcome_future = listObjectsAsync();
        }

        if (request_settings.throw_on_zero_files_match && result_batch.empty())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can not match any files using prefix {}", request.GetPrefix());

        KeysWithInfo temp_buffer;
        temp_buffer.reserve(result_batch.size());

        for (const auto & row : result_batch)
        {
            String key = row.GetKey();
            if (recursive || re2::RE2::FullMatch(key, *matcher))
            {
                S3::ObjectInfo info =
                {
                    .size = size_t(row.GetSize()),
                    .last_modification_time = row.GetLastModified().Millis() / 1000,
                };

                temp_buffer.emplace_back(std::make_shared<KeyWithInfo>(std::move(key), std::move(info)));
            }
        }

        if (temp_buffer.empty())
        {
            buffer_iter = buffer.begin();
            return;
        }

        // if (filter_dag)
        // {
        //     std::vector<String> paths;
        //     paths.reserve(temp_buffer.size());
        //     for (const auto & key_with_info : temp_buffer)
        //         paths.push_back(fs::path(globbed_uri.bucket) / key_with_info->key);

        //     VirtualColumnUtils::filterByPathOrFile(temp_buffer, paths, filter_dag, virtual_columns, getContext());
        // }

        buffer = std::move(temp_buffer);

        // if (file_progress_callback)
        // {
        //     for (const auto & key_with_info : buffer)
        //         file_progress_callback(FileProgress(0, key_with_info->info->size));
        // }

        /// Set iterator only after the whole batch is processed
        buffer_iter = buffer.begin();

        if (read_keys)
            read_keys->insert(read_keys->end(), buffer.begin(), buffer.end());    
    }

    std::future<ListObjectsOutcome> listObjectsAsync()
    {
        return list_objects_scheduler([this]
        {
            ProfileEvents::increment(ProfileEvents::S3ListObjects);
            auto outcome = client->ListObjectsV2(request);

            /// Outcome failure will be handled on the caller side.
            if (outcome.IsSuccess())
                request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

            return outcome;
        }, Priority{});
    }

    std::mutex mutex;

    KeysWithInfo buffer;
    KeysWithInfo::iterator buffer_iter;

    std::vector<String> expanded_keys;
    std::vector<String>::iterator expanded_keys_iter;

    std::unique_ptr<Aws::S3::S3Client> client;
    S3::URI globbed_uri;
    // const ActionsDAG::Node * predicate;
    ASTPtr query;
    NamesAndTypesList virtual_columns;
    // ActionsDAGPtr filter_dag;
    std::unique_ptr<re2::RE2> matcher;
    bool recursive{false};
    bool is_finished_for_key{false};
    KeysWithInfo * read_keys;

    Aws::S3::Model::ListObjectsV2Request request;
    S3Settings::RequestSettings request_settings;

    ThreadPool list_objects_pool;
    ThreadPoolCallbackRunner<ListObjectsOutcome> list_objects_scheduler;
    std::future<ListObjectsOutcome> outcome_future;
    // std::function<void(FileProgress)> file_progress_callback;
};


StorageS3Source::DisclosedGlobIterator::DisclosedGlobIterator(
    const Aws::S3::S3Client & client_,
    const S3::URI & globbed_uri_,
    const NamesAndTypesList & virtual_columns,
    ContextPtr context,
    KeysWithInfo * read_keys_,
    const S3Settings::RequestSettings & request_settings_)
    : pimpl(std::make_shared<StorageS3Source::DisclosedGlobIterator::Impl>(
        client_, globbed_uri_, virtual_columns, context, read_keys_, request_settings_))
{
}

StorageS3Source::KeyWithInfoPtr StorageS3Source::DisclosedGlobIterator::next(size_t idx)
{
    return pimpl->next(idx);
}

size_t StorageS3Source::DisclosedGlobIterator::estimatedKeysCount()
{
    return pimpl->objectsCount();
}

StorageS3Source::ReadTaskIterator::ReadTaskIterator(
    const DB::ReadTaskCallback & callback_,
    size_t max_threads_count)
    : callback(callback_)
{
    ThreadPool pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, CurrentMetrics::StorageS3ThreadsScheduled, max_threads_count);
    auto pool_scheduler = threadPoolCallbackRunner<String>(pool, "S3ReadTaskItr");

    std::vector<std::future<String>> keys;
    keys.reserve(max_threads_count);
    for (size_t i = 0; i < max_threads_count; ++i)
        keys.push_back(pool_scheduler([this] { return callback(); }, Priority{}));

    pool.wait();
    buffer.reserve(max_threads_count);
    for (auto & key_future : keys)
        buffer.emplace_back(std::make_shared<KeyWithInfo>(key_future.get(), std::nullopt));
}

StorageS3Source::KeyWithInfoPtr StorageS3Source::ReadTaskIterator::next(size_t) /// NOLINT
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= buffer.size())
        return std::make_shared<KeyWithInfo>(callback());

    while (current_index < buffer.size())
    {
        if (const auto & key_info = buffer[current_index]; key_info && !key_info->key.empty())
            return buffer[current_index];

        current_index = index.fetch_add(1, std::memory_order_relaxed);
    }

    return nullptr;
}

size_t StorageS3Source::ReadTaskIterator::estimatedKeysCount()
{
    return buffer.size();
}

Block StorageS3Source::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}


StorageS3Source::StorageS3Source(
    const ReadFromFormatInfo & info,
    const String & format_,
    String name_,
    const ContextPtr & context_,
    std::optional<FormatSettings> format_settings_,
    UInt64 max_block_size_,
    const S3Settings::RequestSettings & request_settings_,
    String compression_hint_,
    const std::shared_ptr<Aws::S3::S3Client> & client_,
    const String & bucket_,
    const String & version_id_,
    const String & url_host_and_port_,
    std::shared_ptr<IIterator> file_iterator_,
    const size_t max_parsing_threads_,
    bool need_only_count_)
    : SourceWithProgress(info.source_header, false)
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , url_host_and_port(url_host_and_port_)
    , format(format_)
    , columns_desc(info.columns_description)
    , requested_columns(info.requested_columns)
    , max_block_size(max_block_size_)
    , request_settings(request_settings_)
    , compression_hint(std::move(compression_hint_))
    , client(client_)
    , sample_block(info.format_header)
    , format_settings(format_settings_)
    , requested_virtual_columns(info.requested_virtual_columns)
    , file_iterator(file_iterator_)
    , max_parsing_threads(max_parsing_threads_)
    , need_only_count(need_only_count_)
    , create_reader_pool(
          CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, CurrentMetrics::StorageS3ThreadsScheduled, 1)
{
    initialize();
}

bool StorageS3Source::initialize(size_t idx)
{
    KeyWithInfoPtr key_with_info;
    do
    {
        key_with_info = file_iterator->next(idx);
        if (!key_with_info || key_with_info->key.empty())
            return {};

        if (!key_with_info->info)
            key_with_info->info = S3::getObjectInfo(*client, bucket, key_with_info->key, version_id, request_settings);
    }
    while (getContext()->getSettingsRef().s3_skip_empty_files && key_with_info->info->size == 0);


    file_path = fs::path(bucket) / key_with_info->key;

    read_buf = wrapReadBufferWithCompressionMethod(
        std::make_unique<ReadBufferFromS3>(
            client,
            bucket,
            key_with_info->key,
            getContext()->getReadSettings(),
            request_settings.max_single_read_retries),
            chooseCompressionMethod(key_with_info->key, compression_hint));

    LOG_DEBUG(getLogger("StorageS3Source"), "max parsing threads = {} need_only_count = {}", max_parsing_threads, need_only_count);

    auto input_format = FormatFactory::instance().getInput(
        format, *read_buf, sample_block, getContext(), max_block_size);
    pipeline = std::make_unique<QueryPipeline>();
    pipeline->init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        pipeline->addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext());
        });
    }

    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    return true;
}

String StorageS3Source::getName() const
{
    return name;
}

Chunk StorageS3Source::generate()
{
    if (!reader)
        return {};

    Chunk chunk;
    if (reader->pull(chunk))
    {
        UInt64 num_rows = chunk.getNumRows();

            for (const auto & virtual_column : requested_virtual_columns)
            {
                if (virtual_column.name == "_path")
                {
                    chunk.addColumn(virtual_column.type->createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
                }
                else if (virtual_column.name == "_file")
                {
                    size_t last_slash_pos = file_path.find_last_of('/');
                    auto column = virtual_column.type->createColumnConst(num_rows, file_path.substr(last_slash_pos + 1));
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }
            }

        return chunk;
    }

    reader.reset();
    pipeline.reset();
    read_buf.reset();

    if (!initialize())
        return {};

    return generate();
}


class StorageS3BlockOutputStream : public IBlockOutputStream
{
public:
    StorageS3BlockOutputStream(
        const String & format,
        const Block & sample_block_,
        ContextPtr context,
        const CompressionMethod compression_method,
        const std::shared_ptr<Aws::S3::S3Client> & client,
        const String & bucket,
        const String & key,
        size_t min_upload_part_size,
        size_t max_single_part_upload_size)
        : sample_block(sample_block_)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromS3>(client, bucket, key, min_upload_part_size, max_single_part_upload_size), compression_method, 3);
        writer = FormatFactory::instance().getOutputStreamParallelIfPossible(format, *write_buf, sample_block, context);
    }

    Block getHeader() const override
    {
        return sample_block;
    }

    void write(const Block & block) override
    {
        writer->write(block);
    }

    void writePrefix() override
    {
        writer->writePrefix();
    }

    void flush() override
    {
        writer->flush();
    }

    void writeSuffix() override
    {
        try
        {
            writer->writeSuffix();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            writer.reset();
            throw;
        }
    }

private:
    Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    BlockOutputStreamPtr writer;
};


StorageS3::StorageS3(
    const Configuration & configuration_,
    ContextPtr context_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , configuration(configuration_)
    , name(configuration.url.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
{
    if (format_settings)
        format_settings->avoid_buffering = false;
        
    updateConfiguration(context_);

    FormatFactory::instance().checkFormatName(configuration.format);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(configuration.url.uri);

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        ColumnsDescription columns;
        /// TODO: format detect
        columns = getTableStructureFromData(configuration, format_settings, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    // auto default_virtuals = NamesAndTypesList{
    //     {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
    //     {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    // auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    // virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    // for (const auto & column : virtual_columns)
    //     virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

StorageS3::Configuration StorageS3::updateConfigurationAndGetCopy(ContextPtr local_context)
{
    std::lock_guard lock(configuration_update_mutex);
    configuration.update(local_context);
    return configuration;
}

void StorageS3::updateConfiguration(ContextPtr local_context)
{
    std::lock_guard lock(configuration_update_mutex);
    configuration.update(local_context);
}

void StorageS3::useConfiguration(const Configuration & new_configuration)
{
    std::lock_guard lock(configuration_update_mutex);
    configuration = new_configuration;
}

const StorageS3::Configuration & StorageS3::getConfiguration()
{
    std::lock_guard lock(configuration_update_mutex);
    return configuration;
}

bool StorageS3::Configuration::update(ContextPtr context)
{
    auto s3_settings = context->getStorageS3Settings().getSettings(url.uri.toString());

    if (client && (static_configuration || s3_settings.auth_settings == auth_settings))
        return false;

    auth_settings.updateFrom(s3_settings.auth_settings);
    keys[0] = url.key;
    connect(context);
    return true;
}

void StorageS3::Configuration::connect(ContextPtr ctx)
{
    std::shared_ptr<Aws::Client::ClientConfiguration> client_configuration =
        S3::ClientFactory::instance().createClientConfiguration(
            auth_settings.region,
            ctx->getRemoteHostFilter(), ctx->getGlobalContext()->getSettingsRef().s3_max_redirects,
            ctx->getConfigRef().getUInt("s3.http_keep_alive_timeout_ms", 5000),
            ctx->getConfigRef().getUInt("s3.http_connection_pool_size", 1024), false);

    client_configuration->endpointOverride = url.endpoint;
    client_configuration->maxConnections = static_cast<unsigned>(request_settings.max_connections);
    auto headers = auth_settings.headers;
    if (!headers_from_ast.empty())
        headers.insert(headers.end(), headers_from_ast.begin(), headers_from_ast.end());

    auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.access_key_secret);
    client = S3::ClientFactory::instance().create(
        client_configuration,
        url.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        auth_settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        S3::CredentialsConfiguration
        {
            auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", true)),
            auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
            auth_settings.expiration_window_seconds.value_or(
                ctx->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            auth_settings.no_sign_request.value_or(ctx->getConfigRef().getBool("s3.no_sign_request", false))
        });
}

StorageS3::Configuration StorageS3::getConfiguration(ASTs & engine_args, ContextPtr local_context, bool /*get_format_from_file*/)
{
    StorageS3::Configuration configuration;

    {
        /// Supported signatures:
        ///
        /// S3('url')
        /// S3('url', 'format')
        /// S3('url', 'format', 'compression')
        /// S3('url', NOSIGN)
        /// S3('url', NOSIGN, 'format')
        /// S3('url', NOSIGN, 'format', 'compression')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')
        /// with optional headers() function

        size_t count = evalArgsAndCollectHeaders(engine_args, configuration.headers_from_ast, local_context);
        if (count == 0 || count > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage S3 requires 1 to 5 arguments: "
                            "url, [NOSIGN | access_key_id, secret_access_key], name of used format and [compression_method]");

        std::unordered_map<std::string_view, size_t> engine_args_to_idx;
        bool no_sign_request = false;

        /// For 2 arguments we support 2 possible variants:
        /// - s3(source, format)
        /// - s3(source, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        if (count == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                no_sign_request = true;
            else
                engine_args_to_idx = {{"format", 1}};
        }
        /// For 3 arguments we support 2 possible variants:
        /// - s3(source, format, compression_method)
        /// - s3(source, access_key_id, access_key_id)
        /// - s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or format name.
        else if (count == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "format/access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                engine_args_to_idx = {{"format", 2}};
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};
            else
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        /// For 4 arguments we support 3 possible variants:
        /// - s3(source, access_key_id, secret_access_key, session_token)
        /// - s3(source, access_key_id, secret_access_key, format)
        /// - s3(source, NOSIGN, format, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN or not.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                engine_args_to_idx = {{"format", 2}, {"compression_method", 3}};
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "session_token/format");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
                }
                else
                {
                    engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
                }
            }
        }
        /// For 5 arguments we support 2 possible variants:
        /// - s3(source, access_key_id, secret_access_key, session_token, format)
        /// - s3(source, access_key_id, secret_access_key, format, compression)
        else if (count == 5)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "session_token/format");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression", 4}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
            }
        }
        else if (count == 6)
        {
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}};
        }

        /// This argument is always the first
        configuration.url = S3::URI(checkAndGetLiteralArgument<String>(engine_args[0], "url"));

        if (engine_args_to_idx.contains("format"))
            configuration.format = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["format"]], "format");

        if (engine_args_to_idx.contains("compression_method"))
            configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["compression_method"]], "compression_method");

        if (engine_args_to_idx.contains("access_key_id"))
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["access_key_id"]], "access_key_id");

        if (engine_args_to_idx.contains("secret_access_key"))
            configuration.auth_settings.access_key_secret = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["secret_access_key"]], "secret_access_key");

        if (engine_args_to_idx.contains("session_token"))
            configuration.auth_settings.session_token = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["session_token"]], "session_token");

        configuration.auth_settings.no_sign_request = no_sign_request;
    }

    configuration.static_configuration = !configuration.auth_settings.access_key_id.empty();

    configuration.keys = {configuration.url.key};

    // if (configuration.format == "auto" && get_format_from_file)
    //     configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url.key, true);

    return configuration;
}


ColumnsDescription StorageS3::getTableStructureFromData(
    const StorageS3::Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    return getTableStructureAndFormatFromDataImpl(configuration.format, configuration, format_settings, ctx).first;
}

std::pair<ColumnsDescription, String> StorageS3::getTableStructureAndFormatFromData(
    const StorageS3::Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    return getTableStructureAndFormatFromDataImpl(std::nullopt, configuration, format_settings, ctx);
}

static constexpr auto bad_arguments_error_message = "URL requires 1-4 arguments: "
                                                    "url, name of used format (taken from file extension by default), "
                                                    "optional compression method, optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

size_t StorageS3::evalArgsAndCollectHeaders(
    ASTs & url_function_args, HTTPHeaderEntries & header_entries, const ContextPtr & ctx)
{
    ASTs::iterator headers_it = url_function_args.end();

    for (ASTs::iterator arg_it = url_function_args.begin(); arg_it != url_function_args.end(); ++arg_it)
    {
        const auto * headers_ast_function = (*arg_it)->as<ASTFunction>();
        if (headers_ast_function && headers_ast_function->name == "headers")
        {
            if (headers_it != url_function_args.end())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "URL table function can have only one key-value argument: headers=(). {}",
                    bad_arguments_error_message);

            const auto * headers_function_args_expr = assert_cast<const ASTExpressionList *>(headers_ast_function->arguments.get());
            auto headers_function_args = headers_function_args_expr->children;

            for (auto & header_arg : headers_function_args)
            {
                const auto * header_ast = header_arg->as<ASTFunction>();
                if (!header_ast || header_ast->name != "equals")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Headers argument is incorrect. {}", bad_arguments_error_message);

                const auto * header_args_expr = assert_cast<const ASTExpressionList *>(header_ast->arguments.get());
                auto header_args = header_args_expr->children;
                if (header_args.size() != 2)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Headers argument is incorrect: expected 2 arguments, got {}",
                        header_args.size());

                auto ast_literal = evaluateConstantExpressionOrIdentifierAsLiteral(header_args[0], ctx);
                auto arg_name_value = ast_literal->as<ASTLiteral>()->value;
                if (arg_name_value.getType() != Field::Types::Which::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as header name");
                auto arg_name = arg_name_value.safeGet<String>();

                ast_literal = evaluateConstantExpressionOrIdentifierAsLiteral(header_args[1], ctx);
                auto arg_value = ast_literal->as<ASTLiteral>()->value;
                if (arg_value.getType() != Field::Types::Which::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as header value");

                header_entries.emplace_back(arg_name, arg_value.safeGet<String>());
            }

            headers_it = arg_it;

            continue;
        }

        if (headers_ast_function && headers_ast_function->name == "equals")
            continue;

        (*arg_it) = evaluateConstantExpressionOrIdentifierAsLiteral((*arg_it), ctx);
    }

    if (headers_it == url_function_args.end())
        return url_function_args.size();

    std::rotate(headers_it, std::next(headers_it), url_function_args.end());
    return url_function_args.size() - 1;
}

std::shared_ptr<StorageS3Source::IIterator> StorageS3::createFileIterator(
    const Configuration & configuration,
    bool distributed_processing,
    ContextPtr local_context,
    const NamesAndTypesList & virtual_columns,
    KeysWithInfo * read_keys)
{
    if (distributed_processing)
    {
        return std::make_shared<StorageS3Source::ReadTaskIterator>(
            local_context->getReadTaskCallback(), local_context->getSettingsRef().max_threads);
    }
    else if (configuration.withGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<StorageS3Source::DisclosedGlobIterator>(
            *configuration.client, configuration.url, virtual_columns,
            local_context, read_keys, configuration.request_settings);
    }
    else
    {
        return std::make_shared<StorageS3Source::KeysIterator>(
            *configuration.client, "", configuration.keys,
            configuration.url.bucket, configuration.request_settings, read_keys);
    }
}

Pipe StorageS3::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, false /*supports_subset_of_columns*/);

    auto query_configuration = updateConfigurationAndGetCopy(local_context);

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper = createFileIterator(
        query_configuration, distributed_processing, local_context, {} /*virtual_column*/, nullptr);

    const size_t max_download_threads = local_context->getSettingsRef().max_download_threads;
    Pipes pipes;

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageS3Source>(
            read_from_format_info,
            query_configuration.format,
            getName(),
            local_context,
            format_settings,
            max_block_size,
            query_configuration.request_settings,
            query_configuration.compression_method,
            query_configuration.client,
            query_configuration.url.bucket,
            "" /*version_id*/,
            query_configuration.url.uri.getHost() + std::to_string(query_configuration.url.uri.getPort()),
            iterator_wrapper,
            max_download_threads,
            false /*need_only_count*/));

    }
    auto pipe = Pipe::unitePipes(std::move(pipes));

    narrowPipe(pipe, num_streams);
    return pipe;
}

BlockOutputStreamPtr StorageS3::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto query_configuration = updateConfigurationAndGetCopy(local_context);

    return std::make_shared<StorageS3BlockOutputStream>(
        query_configuration.format,
        metadata_snapshot->getSampleBlock(),
        local_context,
        chooseCompressionMethod(query_configuration.url.key, query_configuration.compression_method),
        query_configuration.client,
        query_configuration.url.bucket,
        query_configuration.url.key,
        query_configuration.request_settings.min_upload_part_size,
        query_configuration.request_settings.max_single_part_upload_size);
}

void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    auto query_configuration = updateConfigurationAndGetCopy(local_context);

    if (query_configuration.withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "S3 key '{}' contains globs, so the table is in readonly mode",
            query_configuration.url.key);
    }

    Aws::S3::Model::Delete delkeys;

    for (const auto & key : query_configuration.keys)
    {
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(key);
        delkeys.AddObjects(std::move(obj));
    }

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    Aws::S3::Model::DeleteObjectsRequest request;
    request.SetBucket(query_configuration.url.bucket);
    request.SetDelete(delkeys);

    auto response = query_configuration.client->DeleteObjects(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(ErrorCodes::S3_ERROR, "{}: {}", std::to_string(static_cast<int>(err.GetErrorType())), err.GetMessage());
    }

    for (const auto & error : response.GetResult().GetErrors())
        LOG_WARNING(getLogger("StorageS3"), "Failed to delete {}, error: {}", error.GetKey(), error.GetMessage());
}

namespace
{
    class ReadBufferIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferIterator(
            std::shared_ptr<StorageS3Source::IIterator> file_iterator_,
            const StorageS3Source::KeysWithInfo & read_keys_,
            const StorageS3::Configuration & configuration_,
            std::optional<String> format_,
            const std::optional<FormatSettings> & format_settings_,
            const ContextPtr & context_)
            : WithContext(context_)
            , file_iterator(file_iterator_)
            , read_keys(read_keys_)
            , configuration(configuration_)
            , format(std::move(format_))
            , format_settings(format_settings_)
            , prev_read_keys_size(read_keys_.size())
        {
        }

        Data next() override
        {
            if (first)
            {
                /// If format is unknown we iterate through all currently read keys on first iteration and
                /// try to determine format by file name.
                if (!format)
                {
                    // for (const auto & key_with_info : read_keys)
                    // {
                    //     if (auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName(key_with_info->key))
                    //     {
                    //         format = format_from_file_name;
                    //         break;
                    //     }
                    // }
                }

                /// For default mode check cached columns for currently read keys on first iteration.
                if (first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
                {
                    LOG_TRACE(getLogger("StorageS3Source"), "ReadBufferIterator first get columns from cache.");

                    if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
                        return {nullptr, cached_columns, format};
                }
            }

            while (true)
            {
                current_key_with_info = (*file_iterator)();

                if (!current_key_with_info || current_key_with_info->key.empty())
                {
                    if (first)
                    {
                        if (format)
                            throw Exception(
                                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                                "The table structure cannot be extracted from a {} format file, because there are no files with provided path "
                                "in S3 or all files are empty. You can specify table structure manually",
                                *format);

                        throw Exception(
                            ErrorCodes::CANNOT_DETECT_FORMAT,
                            "The data format cannot be detected by the contents of the files, because there are no files with provided path "
                            "in S3 or all files are empty. You can specify the format manually");
                    }

                    return {nullptr, std::nullopt, format};
                }

                LOG_TRACE(getLogger("StorageS3Source"), "ReadBufferIterator read_keys size {} prev_read_keys_size = {}", read_keys.size(), prev_read_keys_size);

                /// S3 file iterator could get new keys after new iteration
                if (read_keys.size() > prev_read_keys_size)
                {
                    /// If format is unknown we can try to determine it by new file names.
                    if (!format)
                    {
                        // for (auto it = read_keys.begin() + prev_read_keys_size; it != read_keys.end(); ++it)
                        // {
                        //     if (auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName((*it)->key))
                        //     {
                        //         format = format_from_file_name;
                        //         break;
                        //     }
                        // }
                    }

                    /// Check new files in schema cache if schema inference mode is default.
                    if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
                    {
                        auto columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end());
                        if (columns_from_cache)
                            return {nullptr, columns_from_cache, format};
                    }

                    prev_read_keys_size = read_keys.size();
                }

                if (getContext()->getSettingsRef().s3_skip_empty_files && current_key_with_info->info && current_key_with_info->info->size == 0)
                    continue;

                /// In union mode, check cached columns only for current key.
                if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
                {
                    StorageS3::KeysWithInfo keys = {current_key_with_info};
                    if (auto columns_from_cache = tryGetColumnsFromCache(keys.begin(), keys.end()))
                    {
                        first = false;
                        return {nullptr, columns_from_cache, format};
                    }
                }

                LOG_TRACE(getLogger("StorageS3Source"), "ReadBufferFromS3 bucket {} key {}", configuration.url.bucket, current_key_with_info->key);

                auto impl = std::make_unique<ReadBufferFromS3>(
                    configuration.client,
                    configuration.url.bucket,
                    current_key_with_info->key,
                    getContext()->getReadSettings(),
                    configuration.request_settings.max_single_read_retries);

                if (!getContext()->getSettingsRef().s3_skip_empty_files || !impl->eof())
                {
                    first = false;
                    return {wrapReadBufferWithCompressionMethod(std::move(impl), chooseCompressionMethod(current_key_with_info->key, configuration.compression_method)), std::nullopt, format};
                }
            }
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3)
                return;

            String source = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / configuration.url.bucket / current_key_with_info->key;
            auto key = getKeyForSchemaCache(source, *format, format_settings, getContext());
            StorageS3::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

        void setSchemaToLastFile(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::UNION)
                return;

            String source = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / configuration.url.bucket / current_key_with_info->key;
            auto cache_key = getKeyForSchemaCache(source, *format, format_settings, getContext());
            StorageS3::getSchemaCache(getContext()).addColumns(cache_key, columns);
        }

        void setResultingSchema(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::DEFAULT)
                return;

            auto host_and_bucket = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / configuration.url.bucket;
            Strings sources;
            sources.reserve(read_keys.size());
            std::transform(read_keys.begin(), read_keys.end(), std::back_inserter(sources), [&](const auto & elem){ return host_and_bucket / elem->key; });
            auto cache_keys = getKeysForSchemaCache(sources, *format, format_settings, getContext());
            StorageS3::getSchemaCache(getContext()).addManyColumns(cache_keys, columns);
        }

        void setFormatName(const String & format_name) override
        {
            format = format_name;
        }

        String getLastFileName() const override
        {
            if (current_key_with_info)
                return current_key_with_info->key;
            return "";
        }

        bool supportsLastReadBufferRecreation() const override { return true; }

        std::unique_ptr<ReadBuffer> recreateLastReadBuffer() override
        {
            chassert(current_key_with_info);
            auto impl = std::make_unique<ReadBufferFromS3>(
                configuration.client,
                configuration.url.bucket,
                current_key_with_info->key,
                getContext()->getReadSettings(),
                configuration.request_settings.max_single_read_retries);
            return wrapReadBufferWithCompressionMethod(std::move(impl), chooseCompressionMethod(current_key_with_info->key, configuration.compression_method));
        }

    private:
        std::optional<ColumnsDescription> tryGetColumnsFromCache(
            const StorageS3::KeysWithInfo::const_iterator & begin,
            const StorageS3::KeysWithInfo::const_iterator & end)
        {
            auto context = getContext();
            if (!context->getSettingsRef().schema_inference_use_cache_for_s3)
                return std::nullopt;

            auto & schema_cache = StorageS3::getSchemaCache(context);
            for (auto it = begin; it < end; ++it)
            {
                auto get_last_mod_time = [&]
                {
                    time_t last_modification_time = 0;
                    if ((*it)->info)
                    {
                        last_modification_time = (*it)->info->last_modification_time;
                    }
                    else
                    {
                        /// Note that in case of exception in getObjectInfo returned info will be empty,
                        /// but schema cache will handle this case and won't return columns from cache
                        /// because we can't say that it's valid without last modification time.
                        last_modification_time = S3::getObjectInfo(
                             *configuration.client,
                             configuration.url.bucket,
                             (*it)->key,
                             /*version_id*/ {},
                             configuration.request_settings,
                             /*with_metadata=*/ false,
                             /*throw_on_error= */ false).last_modification_time;
                    }

                    return last_modification_time ? std::make_optional(last_modification_time) : std::nullopt;
                };

                String path = fs::path(configuration.url.bucket) / (*it)->key;
                String source = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / path;

                if (format)
                {
                    auto cache_key = getKeyForSchemaCache(source, *format, format_settings, context);
                    if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                        return columns;
                }
                else
                {
                    /// TODO: detect format
                    // /// If format is unknown, we can iterate through all possible input formats
                    // /// and check if we have an entry with this format and this file in schema cache.
                    // /// If we have such entry for some format, we can use this format to read the file.
                    // for (const auto & format_name : FormatFactory::instance().getAllInputFormats())
                    // {
                    //     auto cache_key = getKeyForSchemaCache(source, format_name, format_settings, context);
                    //     if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                    //     {
                    //         /// Now format is known. It should be the same for all files.
                    //         format = format_name;
                    //         return columns;
                    //     }
                    // }
                }
            }

            return std::nullopt;
        }

        std::shared_ptr<StorageS3Source::IIterator> file_iterator;
        const StorageS3Source::KeysWithInfo & read_keys;
        const StorageS3::Configuration & configuration;
        std::optional<String> format;
        const std::optional<FormatSettings> & format_settings;
        StorageS3Source::KeyWithInfoPtr current_key_with_info;
        size_t prev_read_keys_size;
        bool first = true;
    };

}

std::pair<ColumnsDescription, String> StorageS3::getTableStructureAndFormatFromDataImpl(
    std::optional<String> format,
    const Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    KeysWithInfo read_keys;
    LOG_TRACE(getLogger("StorageS3"), " getTableStructureAndFormatFromDataImpl start createFileIterator ");
    auto file_iterator = createFileIterator(configuration, false, ctx, {}, &read_keys);

    ReadBufferIterator read_buffer_iterator(file_iterator, read_keys, configuration, format, format_settings, ctx);
    if (format)
        return {readSchemaFromFormat(*format, format_settings, read_buffer_iterator, ctx), *format};
    return detectFormatAndReadSchema(format_settings, read_buffer_iterator, ctx);
}

void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [](const StorageFactory::Arguments & args)
    {   
        auto & engine_args = args.engine_args;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

        auto configuration = StorageS3::getConfiguration(engine_args, args.getLocalContext());
        // Use format settings from global server context + settings from
        // the SETTINGS clause of the create query. Settings from current
        // session and user are ignored.
        std::optional<FormatSettings> format_settings;
        if (args.storage_def->settings)
        {
            FormatFactorySettings user_format_settings;

            // Apply changed settings from global context, but ignore the
            // unknown ones, because we only have the format settings here.
            const auto & changes = args.getContext()->getSettingsRef().changes();
            for (const auto & change : changes)
            {
                if (user_format_settings.has(change.name))
                    user_format_settings.set(change.name, change.value);
            }

            // Apply changes from SETTINGS clause, with validation.
            user_format_settings.applyChanges(args.storage_def->settings->changes);
            format_settings = getFormatSettings(args.getContext(), user_format_settings);
        }
        else
        {
            format_settings = getFormatSettings(args.getContext());
        }

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return StorageS3::create(
            std::move(configuration),
            args.getContext(),
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            format_settings,
            /* distributed_processing_ */false,
            partition_by);
    },
    {
        .source_access_type = AccessType::S3,
    });
}

void registerStorageS3(StorageFactory & factory)
{
    return registerStorageS3Impl("S3", factory);
}

void registerStorageCOS(StorageFactory & factory)
{
    return registerStorageS3Impl("COSN", factory);
}

NamesAndTypesList StorageS3::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}

SchemaCache & StorageS3::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_s3", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

}

#endif
