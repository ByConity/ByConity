#include <Common/config.h>

#if USE_AWS_S3
#    include "StorageCloudS3.h"

#    include <filesystem>
#    include <utility>
#    include <DataStreams/IBlockInputStream.h>
#    include <DataStreams/PartitionedBlockOutputStream.h>
#    include <DataStreams/UnionBlockInputStream.h>
#    include <IO/ReadBufferFromS3.h>
#    include <IO/RAReadBufferFromS3.h>
#    include <IO/WriteBufferFromByteS3.h>
#    include "Interpreters/Context.h"
#    include <IO/WriteBufferFromS3.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTInsertQuery.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/StorageFactory.h>
#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/CopyObjectRequest.h>
#    include <aws/s3/model/DeleteObjectsRequest.h>
#    include <aws/s3/model/ListObjectsV2Request.h>
#    include <re2/stringpiece.h>
#    include <Common/Exception.h>

namespace DB
{

std::unique_ptr<ReadBuffer> StorageCloudS3::FileBufferClient::createReadBuffer(const DB::String & key)
{
    if (config.use_read_ahead)
        return std::make_unique<RAReadBufferFromS3>(config.client, config.uri.bucket, key, config.rw_settings.max_single_read_retries);
    return std::make_unique<ReadBufferFromS3>(config.client, config.uri.bucket, key, ReadSettings{}, config.rw_settings.max_single_read_retries);
}

std::unique_ptr<WriteBuffer> StorageCloudS3::FileBufferClient::createWriteBuffer(const DB::String & key)
{
    return std::make_unique<WriteBufferFromByteS3>(config.client, config.uri.bucket, key, config.rw_settings.max_single_part_upload_size, config.rw_settings.min_upload_part_size, std::nullopt, true);
}

bool StorageCloudS3::FileBufferClient::exist(const DB::String & key)
{
    bool is_finished = false;
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    request.SetBucket(config.uri.bucket);
    request.SetPrefix(key);
    while (!is_finished)
    {
        outcome = config.client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(
                fmt::format("get {} object list failed, error = {}", config.uri.toString(), outcome.GetError().GetMessage()),
                ErrorCodes::LOGICAL_ERROR);

        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & obj : result_batch)
        {
            if (obj.GetKey() == key)
                return true;
        }
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }
    return false;
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
            &Poco::Logger::get("StorageCloudS3"),
            fmt::format(
                "create cloud S3 table: database={}, table={}, url={}, format={}, compression={}",
                database,
                table,
                arguments.url,
                arguments.format_name,
                arguments.compression_method));

        StorageS3Configuration config(arguments.url);
        Strings files{config.uri.key};
        config.updateS3Client(args.getLocalContext(), arguments);
        return StorageCloudS3::create(args.getContext(), args.table_id, args.columns, args.constraints, files, args.storage_def->settings->ptr(), arguments, settings, config);
    },
    features);
}
}
#endif
