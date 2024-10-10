#include <IO/S3Common.h>
#include <Storages/RemoteFile/StorageCnchS3.h>
#include <Storages/StorageS3Settings.h>
#include <Common/config.h>

#if USE_AWS_S3
#    include "StorageCloudS3.h"

#    include <filesystem>
#    include <utility>
#    include <DataStreams/IBlockInputStream.h>
#    include <DataStreams/PartitionedBlockOutputStream.h>
#    include <DataStreams/UnionBlockInputStream.h>
#    include <IO/ReadBufferFromS3.h>
#    include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#    include <IO/WriteBufferFromS3.h>
#    include <Interpreters/Context.h>
#    include <IO/WriteBufferFromS3.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTInsertQuery.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/StorageFactory.h>
#    include <re2/stringpiece.h>
#    include <Common/Exception.h>


namespace DB
{

std::unique_ptr<ReadBuffer> StorageCloudS3::FileBufferClient::createReadBuffer(const DB::String & key)
{
    const ReadSettings & read_settings = context->getReadSettings();
    const S3Settings::RequestSettings s3_request_settings(context->getSettingsRef());
    if (read_settings.remote_fs_prefetch)
    {
        auto impl = std::make_unique<ReadBufferFromS3>(
            s3_util->getClient(),
            s3_util->getBucket(),
            key,
            read_settings,
            s3_request_settings.max_single_read_retries,
            false,
            /* use_external_buffer */ true);

        auto global_context = Context::getGlobalContextInstance();
        auto reader = global_context->getThreadPoolReader();
        // Create a read buffer that will prefetch the first ~1 MB of the file.
        // When reading lots of tiny files, this prefetching almost doubles the throughput.
        // For bigger files, parallel reading is more useful.
        auto async_buffer = std::make_unique<AsynchronousBoundedReadBuffer>(
            std::move(impl), *reader, read_settings);

        async_buffer->setReadUntilEnd();
        async_buffer->prefetch(Priority{});

        return async_buffer;
    }
    else
    {
        return std::make_unique<ReadBufferFromS3>(
            s3_util->getClient(), s3_util->getBucket(), key, read_settings, s3_request_settings.max_single_read_retries, false);
    }
}

std::unique_ptr<WriteBuffer> StorageCloudS3::FileBufferClient::createWriteBuffer(const DB::String & key)
{
    S3Settings::RequestSettings request_settings(context->getSettingsRef());
    return std::make_unique<WriteBufferFromS3>(
        s3_util->getClient(),
        s3_util->getBucket(),
        key,
        request_settings.max_single_part_upload_size,
        request_settings.min_upload_part_size,
        std::nullopt,
        DBMS_DEFAULT_BUFFER_SIZE,
        true,
        request_settings.s3_use_parallel_upload,
        request_settings.s3_parallel_upload_pool_size);
}

bool StorageCloudS3::FileBufferClient::exist(const DB::String & key)
{
    return s3_util->exists(key);
}

void registerStorageCloudS3(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
    };

    factory.registerStorage("CloudS3", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5 && engine_args.size() != 7)
            throw Exception(
                "Storage CnchS3 requires exactly 5 or 7 arguments on workers: database_name, table_name, url, "
                "format, compression and [aws_access_key_id, aws_secret_access_key]",
                ErrorCodes::LOGICAL_ERROR);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.getLocalContext());
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.getLocalContext());
        engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.getLocalContext());
        engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.getLocalContext());

        String database = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String table = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        CnchFileArguments arguments;
        arguments.url = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        arguments.format_name = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        arguments.compression_method = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        if (engine_args.size() == 7)
        {
            engine_args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[5], args.getLocalContext());
            engine_args[6] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[6], args.getLocalContext());
            arguments.access_key_id = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
            arguments.access_key_secret = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();
        }

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            arguments.partition_by = args.storage_def->partition_by->clone();

        CnchFileSettings settings = args.getContext()->getCnchFileSettings();
        settings.loadFromQuery(*args.storage_def);
        LOG_TRACE(
            getLogger("StorageCloudS3"),
            fmt::format(
                "create cloud S3 table: database={}, table={}, url={}, format={}, compression={}",
                database,
                table,
                arguments.url,
                arguments.format_name,
                arguments.compression_method));

        S3::URI s3_uri(arguments.url);
        Strings files{s3_uri.key};
        S3ClientPtr client = initializeS3Client(args.getLocalContext(), arguments);
        std::shared_ptr<S3::S3Util> s3_util = std::make_shared<S3::S3Util>(client, s3_uri.bucket);
        return StorageCloudS3::create(
            args.getContext(),
            args.table_id,
            args.columns,
            args.constraints,
            files,
            args.storage_def->settings->ptr(),
            arguments,
            settings,
            s3_util);
    },
    features);
}
}
#endif
