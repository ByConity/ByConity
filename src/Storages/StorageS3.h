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

#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Poco/URI.h>
#include <common/logger_useful.h>
#include <common/shared_ptr_helper.h>
#include <IO/S3Common.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Interpreters/threadPoolCallbackRunner.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{

class PullingPipelineExecutor;
class StorageS3SequentialSource;
class SchemaCache;
struct ReadFromFormatInfo;

class StorageS3Source : public SourceWithProgress, WithContext
{
public:
    struct KeyWithInfo
    {
        KeyWithInfo() = default;
        KeyWithInfo(String key_, std::optional<S3::ObjectInfo> info_ = std::nullopt)
            : key(std::move(key_)), info(std::move(info_))
        {
        }
        virtual ~KeyWithInfo() = default;

        String key;
        std::optional<S3::ObjectInfo> info;
    };
    
    using KeyWithInfoPtr = std::shared_ptr<KeyWithInfo>;

    using KeysWithInfo = std::vector<KeyWithInfoPtr>;

    class IIterator
    {
    public:
        virtual ~IIterator() = default;
        virtual KeyWithInfoPtr next(size_t idx = 0) = 0;
        /// Estimates how many streams we need to process all files.
        /// If keys count >= max_threads_count, the returned number may not represent the actual number of the keys.
        /// Intended to be called before any next() calls, may underestimate otherwise
        /// fixme: May underestimate if the glob has a strong filter, so there are few matches among the first 1000 ListObjects results.
        virtual size_t estimatedKeysCount() = 0;

        KeyWithInfoPtr operator ()() { return next(); }
    };

    class DisclosedGlobIterator : public IIterator
    {
    public:
        DisclosedGlobIterator(
            const Aws::S3::S3Client & client_,
            const S3::URI & globbed_uri_,
            const NamesAndTypesList & virtual_columns,
            ContextPtr context,
            KeysWithInfo * read_keys_ = nullptr,
            const S3Settings::RequestSettings & request_settings_ = {});

        KeyWithInfoPtr next(size_t idx = 0) override;
        size_t estimatedKeysCount() override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class KeysIterator : public IIterator
    {
    public:
        explicit KeysIterator(
            const Aws::S3::S3Client & client_,
            const std::string & version_id_,
            const std::vector<String> & keys_,
            const String & bucket_,
            const S3Settings::RequestSettings & request_settings_,
            KeysWithInfo * read_keys = nullptr);

        KeyWithInfoPtr next(size_t idx = 0) override;
        size_t estimatedKeysCount() override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class ReadTaskIterator : public IIterator
    {
    public:
        explicit ReadTaskIterator(const ReadTaskCallback & callback_, size_t max_threads_count);

        KeyWithInfoPtr next(size_t idx = 0) override;

        size_t estimatedKeysCount() override;

    private:
        KeysWithInfo buffer;
        std::atomic_size_t index = 0;

        ReadTaskCallback callback;
    };

    using IteratorWrapper = std::function<String()>;

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    StorageS3Source(
        const ReadFromFormatInfo & info,
        const String & format,
        String name_,
        const ContextPtr & context_,
        std::optional<FormatSettings> format_settings_,
        UInt64 max_block_size_,
        const S3Settings::RequestSettings & request_settings_,
        String compression_hint_,
        const std::shared_ptr<Aws::S3::S3Client> & client_,
        const String & bucket,
        const String & version_id,
        const String & url_host_and_port,
        std::shared_ptr<IIterator> file_iterator_,
        const size_t max_parsing_threads_,
        bool need_only_count_);

    String getName() const override;

    Chunk generate() override;

private:
    String name;
    String bucket;
    String file_path;
    String version_id;
    String url_host_and_port;
    String format;
    ColumnsDescription columns_desc;
    NamesAndTypesList requested_columns;
    UInt64 max_block_size;
    S3Settings::RequestSettings request_settings;
    String compression_hint;
    std::shared_ptr<Aws::S3::S3Client> client;
    Block sample_block;
    std::optional<FormatSettings> format_settings;

    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;

    NamesAndTypesList requested_virtual_columns;
    std::shared_ptr<IIterator> file_iterator;
    size_t max_parsing_threads = 1;
    bool need_only_count;

    ThreadPool create_reader_pool;
    // ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    // std::atomic<bool> initialized{false};

    // size_t total_rows_in_file = 0;

    /// Recreate ReadBuffer and BlockInputStream for each file.
    bool initialize(size_t idx = 0);
};

/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public shared_ptr_helper<StorageS3>, public IStorage, WithContext
{
public:
 struct Configuration : public StatelessTableEngineConfiguration
    {
        Configuration() = default;

        String getPath() const { return url.key; }

        void appendToPath(const String & suffix)
        {
            url = S3::URI{std::filesystem::path(url.uri.toString()) / suffix};
        }

        bool update(ContextPtr context);

        void connect(ContextPtr context);

        bool withGlobs() const { return url.key.find_first_of("*?{") != std::string::npos; }

        bool withWildcard() const
        {
            static const String PARTITION_ID_WILDCARD = "{_partition_id}";
            return url.bucket.find(PARTITION_ID_WILDCARD) != String::npos
                || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
        }

        S3::URI url;
        S3::AuthSettings auth_settings;
        S3Settings::RequestSettings request_settings;
        /// If s3 configuration was passed from ast, then it is static.
        /// If from config - it can be changed with config reload.
        bool static_configuration = true;
        /// Headers from ast is a part of static configuration.
        HTTPHeaderEntries headers_from_ast;

        std::shared_ptr<Aws::S3::S3Client> client;
        std::vector<String> keys;
    };

    StorageS3(
        const Configuration & configuration_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr);

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static StorageS3::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file = true);

    static ColumnsDescription getTableStructureFromData(
        const StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);
    
    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromData(
        const StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);
    
    /// Does evaluateConstantExpressionOrIdentifierAsLiteral() on all arguments.
    /// If `headers(...)` argument is present, parses it and moves it to the end of the array.
    /// Returns number of arguments excluding `headers(...)`.
    static size_t evalArgsAndCollectHeaders(ASTs & url_function_args, HTTPHeaderEntries & header_entries, const ContextPtr & context);


    using KeysWithInfo = StorageS3Source::KeysWithInfo;

protected:
    virtual Configuration updateConfigurationAndGetCopy(ContextPtr local_context);

    virtual void updateConfiguration(ContextPtr local_context);

    void useConfiguration(const Configuration & new_configuration);

    const Configuration & getConfiguration();

private:

    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;

    Configuration configuration;
    std::mutex configuration_update_mutex;

    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;

    static std::shared_ptr<StorageS3Source::IIterator> createFileIterator(
        const Configuration & configuration,
        bool distributed_processing,
        ContextPtr local_context,
        const NamesAndTypesList & virtual_columns,
        KeysWithInfo * read_keys = nullptr);

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromDataImpl(
        std::optional<String> format,
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);
};

}

#endif
