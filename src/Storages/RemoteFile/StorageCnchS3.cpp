#include "common/types.h"
#include <Common/config.h>

#if USE_AWS_S3
#    include "StorageCnchS3.h"

#    include <filesystem>
#    include <utility>
#    include <DataStreams/RemoteBlockInputStream.h>
#    include <Formats/FormatFactory.h>
#    include <IO/S3Common.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/InterpreterSelectQuery.h>
#    include <Interpreters/RequiredSourceColumnsVisitor.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Interpreters/predicateExpressionsUtils.h>
#    include <Interpreters/trySetVirtualWarehouse.h>
#    include <CloudServices/CnchServerResource.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/ASTSetQuery.h>
#    include <Storages/AlterCommands.h>
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <Storages/RemoteFile/CnchFileSettings.h>
#    include <Storages/RemoteFile/StorageCloudS3.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/VirtualColumnUtils.h>
#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/ListObjectsV2Request.h>
#    include <re2/re2.h>
#    include <re2/stringpiece.h>
#    include <Common/Exception.h>
#    include <Common/parseGlobs.h>

namespace ProfileEvents
{
    extern const Event S3ListObjects;
}

namespace DB
{

Strings ListKeysWithRegexpMatching(
    const std::shared_ptr<const Aws::S3::S3Client> client, const S3::URI & globbed_s3_uri, const UInt64 max_list_nums)
{
    Strings keys;

    if (globbed_s3_uri.bucket.find_first_of("*?{") != DB::String::npos)
        throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::LOGICAL_ERROR);

    const String key_prefix = globbed_s3_uri.key.substr(0, globbed_s3_uri.key.find_first_of("*?{"));
    /// We don't have to list bucket, because there is no asterisks.
    if (key_prefix.size() == globbed_s3_uri.key.size())
    {
        keys.emplace_back(globbed_s3_uri.key);
    }

    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished = false;
    request.SetBucket(globbed_s3_uri.bucket);
    request.SetPrefix(key_prefix);
    request.SetMaxKeys(max_list_nums);

    auto matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(globbed_s3_uri.key));
    UInt64 total_keys = 0;
    while (!is_finished)
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(
                fmt::format("List {} object with prefix {} failed, error = {}",
                globbed_s3_uri.toString(),
                key_prefix,
                outcome.GetError().GetMessage()),
                ErrorCodes::LOGICAL_ERROR);

        total_keys += outcome.GetResult().GetKeyCount();
        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & object : result_batch)
        {
            const String & key = object.GetKey();
            if (re2::RE2::FullMatch(key, *matcher) && key.back() != '/')
                keys.emplace_back(key);
        }
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    LOG_TRACE(
        &Poco::Logger::get("StorageCnchS3"),
        "List {} with prefix `{}`, total keys = {}, filter keys = {} ",
        globbed_s3_uri.toString(),
        key_prefix,
        total_keys,
        keys.size());
    return keys;
}

void StorageCnchS3::readByLocal(
        FileDataPartsCNCHVector parts,
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
{
    auto storage = StorageCloudS3::create(getContext(), getStorageID(), storage_snapshot->metadata->getColumns(), storage_snapshot->metadata->getConstraints(), file_list, storage_snapshot->metadata->getSettingsChanges(), arguments, settings, config);
    storage->loadDataParts(parts);
    return storage->read(query_plan, column_names, storage_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
}

Strings StorageCnchS3::readFileList()
{
    if (arguments.is_glob_path)
        return ListKeysWithRegexpMatching(config.client, config.uri, config.max_list_nums);
    return file_list;
}


BlockOutputStreamPtr StorageCnchS3::writeByLocal(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    /// cnch table write only support server local
    auto storage = StorageCloudS3::create(getContext(), getStorageID(), metadata_snapshot->getColumns(), metadata_snapshot->getConstraints(), file_list, metadata_snapshot->getSettingsChanges(), arguments, settings, config);
    auto streams = storage->write(query, metadata_snapshot, query_context);
    /// todo(jiashuo): insert new file and update the new file list in cache
    // file_list = storage->file_list;
    return streams;
}

void StorageCnchS3::tryUpdateFSClient(const ContextPtr & query_context)
{
    const auto & settings = query_context->getSettingsRef();
    if (settings.s3_access_key_id.changed || settings.s3_access_key_secret.changed || settings.s3_max_redirects.changed
        || settings.s3_check_objects_after_upload.changed || settings.s3_max_connections.changed
        || settings.s3_max_single_part_upload_size.changed || settings.s3_max_single_read_retries.changed
        || settings.s3_max_unexpected_write_error_retries.changed || settings.s3_min_upload_part_size.changed
        || settings.s3_upload_part_size_multiply_factor.changed || settings.s3_upload_part_size_multiply_parts_count_threshold.changed)
    {
        config.updateS3Client(query_context, arguments);
        config.updated = true;
    }
    else if (config.updated)
    {
        config.updateS3Client(query_context, arguments);
        config.updated = false;
    }
}

void registerStorageCnchS3(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
    };

    factory.registerStorage("CnchS3", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 1 && engine_args.size() != 2 && engine_args.size() != 3 && engine_args.size() != 5)
            throw Exception(
                "Storage CnchS3 requires exactly 1, 2, 3 or 5 arguments on server: url, [format], [compression] and [aws_access_key_id, "
                "aws_secret_access_key]",
                ErrorCodes::BAD_ARGUMENTS);

        CnchFileArguments arguments;

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.getLocalContext());
        arguments.url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        if (engine_args.size() >= 2)
        {
            engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());
            arguments.format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        }

        if (engine_args.size() >= 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.getLocalContext());
            arguments.compression_method = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }

        if (engine_args.size() == 5)
        {
            engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.getLocalContext());
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.getLocalContext());
            arguments.access_key_id = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
            arguments.access_key_secret = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        }

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            arguments.partition_by = args.storage_def->partition_by->clone();

        LOG_TRACE(
            &Poco::Logger::get("StorageCnchS3"),
            fmt::format(
                "create CNCH S3 table: url={}, format={}, compression={}",
                arguments.url,
                arguments.format_name,
                arguments.compression_method));

        CnchFileSettings settings = args.getContext()->getCnchFileSettings();
        settings.loadFromQuery(*args.storage_def);
        return StorageCnchS3::create(args.getContext(), args.table_id, args.columns, args.constraints, args.storage_def->settings->ptr(), arguments, settings);
    },
    features);
}
}
#endif
