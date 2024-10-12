#include <Storages/StorageS3Settings.h>
#include <common/logger_useful.h>
#include <common/types.h>
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
#    include <DataStreams/PartitionedBlockOutputStream.h>

namespace ProfileEvents
{
    extern const Event S3ListObjects;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_NOT_FOUND;
}

S3ClientPtr initializeS3Client(const ContextPtr & ctx, const CnchFileArguments & arguments)
{
    S3::URI uri(arguments.url, true);
    if (uri.bucket.find_first_of("*?{") != DB::String::npos)
        throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto s3_settings = ctx->getStorageS3Settings().getSettings(uri.endpoint);
    S3Settings::RequestSettings & request_settings = s3_settings.request_settings;
    request_settings.updateFromSettingsIfEmpty(ctx->getSettingsRef());
    S3::AuthSettings & auth_settings = s3_settings.auth_settings;

    if (!ctx->getSettingsRef().s3_access_key_id.value.empty())
        auth_settings.access_key_id = ctx->getSettingsRef().s3_access_key_id;
    else if (!arguments.access_key_id.empty())
        auth_settings.access_key_id = arguments.access_key_id;

    if (!ctx->getSettingsRef().s3_access_key_secret.value.empty())
        auth_settings.access_key_secret = ctx->getSettingsRef().s3_access_key_secret;
    else if (!arguments.access_key_secret.empty())
        auth_settings.access_key_secret = arguments.access_key_secret;

    std::shared_ptr<Aws::Client::ClientConfiguration> client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings.region,
        ctx->getRemoteHostFilter(),
        static_cast<unsigned>(ctx->getSettingsRef().s3_max_redirects),
        ctx->getConfigRef().getUInt("s3.http_keep_alive_timeout_ms", 5000),
        ctx->getConfigRef().getUInt("s3.http_connection_pool_size", 1024),
        false);

    client_configuration->endpointOverride = uri.endpoint.empty() ? s3_settings.endpoint : uri.endpoint;
    client_configuration->maxConnections = static_cast<unsigned>(request_settings.max_connections);
    client_configuration->region = auth_settings.region.empty() ? uri.region : auth_settings.region;
    client_configuration->connectTimeoutMs = request_settings.max_timeout_ms;
    client_configuration->requestTimeoutMs = request_settings.max_timeout_ms;

    auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.access_key_secret);
    auto headers = auth_settings.headers;

    LOG_DEBUG(
        getLogger("StorageCnchS3"),
        fmt::format(
            "initialize s3 client, config: {}, region = {}, endpoint = {}, bucket = {}, key = {}, ak/sk = {} -> {}",
            uri.toString(),
            auth_settings.region,
            uri.endpoint,
            uri.bucket,
            uri.key,
            auth_settings.access_key_id,
            auth_settings.access_key_secret));

    return S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        auth_settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        S3::CredentialsConfiguration{
            auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", true)),
            auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
            auth_settings.expiration_window_seconds.value_or(
                ctx->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            auth_settings.no_sign_request.value_or(ctx->getConfigRef().getBool("s3.no_sign_request", false))});
}

FilePartInfos ListKeysWithRegexpMatching(const std::shared_ptr<const Aws::S3::S3Client> client, const S3::URI & globbed_s3_uri)
{
    FilePartInfos file_infos;

    if (globbed_s3_uri.bucket.find_first_of("*?{") != DB::String::npos)
        throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::LOGICAL_ERROR);

    const String key_prefix = globbed_s3_uri.key.substr(0, globbed_s3_uri.key.find_first_of("*?{"));
    /// We don't have to list bucket, because there is no asterisks.
    if (key_prefix.size() == globbed_s3_uri.key.size())
    {
        file_infos.emplace_back(globbed_s3_uri.key, 0);
        return file_infos;
    }

    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished = false;
    request.SetBucket(globbed_s3_uri.bucket);
    request.SetPrefix(key_prefix);

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
            const size_t object_size = object.GetSize();
            if (re2::RE2::FullMatch(key, *matcher) && key.back() != '/')
                file_infos.emplace_back(key, object_size);
        }
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    LOG_TRACE(
        getLogger("StorageCnchS3"),
        "List {} with prefix `{}`, total keys = {}, filter keys = {} ",
        globbed_s3_uri.toString(),
        key_prefix,
        total_keys,
        file_infos.size());
    return file_infos;
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
    auto storage = StorageCloudS3::create(
        query_context,
        getStorageID(),
        storage_snapshot->metadata->getColumns(),
        storage_snapshot->metadata->getConstraints(),
        file_list,
        storage_snapshot->metadata->getSettingsChanges(),
        arguments,
        settings,
        s3_util);
    storage->loadDataParts(parts);
    return storage->read(query_plan, column_names, storage_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
}

FilePartInfos StorageCnchS3::readFileList(ContextPtr)
{
    if (arguments.is_glob_path)
        return ListKeysWithRegexpMatching(s3_util->getClient(), s3_uri);

    // Check file exists and get file size
    for (auto & file : file_list)
    {
        try
        {
            auto outcome = s3_util->headObject(file.name);
            file.size = outcome.GetContentLength();
        }
        catch (...)
        {
            throw Exception(ErrorCodes::FILE_NOT_FOUND, "File {} not exist", file.name);
        }
    }
    return file_list;
}

void StorageCnchS3::clear(ContextPtr) 
{
    if (s3_uri.key.find(PartitionedBlockOutputStream::PARTITION_ID_WILDCARD) != String::npos)
    {
        auto key_prefix = s3_uri.key.substr(0, s3_uri.key.find_first_of("*?{"));
        s3_util->deleteObjectsWithPrefix(key_prefix, [](const S3::S3Util &, const String &){return true;});
        LOG_WARNING(log, "You now clear the {} dir {}", getStorageID().getNameForLogs(), s3_uri.toString());
    }
}


BlockOutputStreamPtr StorageCnchS3::writeByLocal(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    /// cnch table write only support server local
    auto storage = StorageCloudS3::create(
        query_context,
        getStorageID(),
        metadata_snapshot->getColumns(),
        metadata_snapshot->getConstraints(),
        file_list,
        metadata_snapshot->getSettingsChanges(),
        arguments,
        settings,
        s3_util);
    auto streams = storage->write(query, metadata_snapshot, query_context);
    /// todo(jiashuo): insert new file and update the new file list in cache
    // file_list = storage->file_list;
    return streams;
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
            getLogger("StorageCnchS3"),
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
