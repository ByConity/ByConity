#include <Common/config.h>

#include <IO/OutfileCommon.h>
#include <IO/WriteBuffer.h>

#include <Interpreters/Context.h>
#include <Common/config.h>
#include <IO/VETosCommon.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromFile.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>

#include <memory>
#include <string>

#if USE_AWS_S3
#    include <IO/WriteBufferFromS3.h>
#    include <IO/S3Common.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_OUTPUT_PATH;
    extern const int INTO_OUTFILE_NOT_ALLOWED;
    extern const int BAD_ARGUMENTS;
}

OutfileTarget::OutfileTarget(
    std::string uri_,
    std::string format_,
    std::string compression_method_str_,
    int compression_level_)
    : uri(uri_)
    , format(format_)
    , compression_method_str(compression_method_str_)
    , compression_level(compression_level_)
{
}

OutfileTarget::OutfileTarget(const OutfileTarget & outfile_target_)
    : uri(outfile_target_.uri)
    , format(outfile_target_.format)
    , compression_method_str(outfile_target_.compression_method_str)
    , compression_level(outfile_target_.compression_level)
{
}

WriteBuffer * OutfileTarget::getOutfileBuffer(const ContextPtr & context, bool allow_into_local)
{
    const Poco::URI out_uri(uri);
    const String & scheme = out_uri.getScheme();
    CompressionMethod compression_method = chooseCompressionMethod(uri, compression_method_str);

    if (scheme.empty())
    {
        if (!allow_into_local)
            throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

        out_buf_raw = std::make_unique<WriteBufferFromFile>(uri, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
    }
#if USE_HDFS
    else if (DB::isHdfsOrCfsScheme(scheme))
    {
        out_buf_raw = std::make_unique<WriteBufferFromHDFS>(
            uri, context->getHdfsConnectionParams(), context->getSettingsRef().max_hdfs_write_buffer_size);

        // hdfs always use CompressionMethod::Gzip default
        if (compression_method_str.empty())
        {
            compression_method = CompressionMethod::Gzip;
        }
    }
#endif
#if USE_AWS_S3
    else if (scheme == "vetos")
    {
        VETosConnectionParams vetos_connect_params = VETosConnectionParams::getVETosSettingsFromContext(context);
        auto tos_uri = verifyTosURI(uri);
        std::string bucket = tos_uri.getHost();
        std::string key = getTosKeyFromURI(tos_uri);
        S3::S3Config s3_config = VETosConnectionParams::getS3Config(vetos_connect_params, bucket);
        out_buf_raw = std::make_unique<WriteBufferFromS3>(s3_config.create(), bucket, key,
            context->getSettingsRef().s3_min_upload_part_size, context->getSettingsRef().s3_max_single_part_upload_size);
    }
    else if (isS3URIScheme(scheme))
    {
        S3::URI s3_uri(out_uri);
        String endpoint = s3_uri.endpoint.empty() ?
            context->getSettingsRef().s3_endpoint.toString() : s3_uri.endpoint;
        S3::S3Config s3_cfg(endpoint, context->getSettingsRef().s3_region.toString(),
            s3_uri.bucket, context->getSettingsRef().s3_ak_id, context->getSettingsRef().s3_ak_secret,
            "");
        out_buf_raw = std::make_unique<WriteBufferFromS3>(s3_cfg.create(), s3_cfg.bucket, s3_uri.key,
            context->getSettingsRef().s3_min_upload_part_size, context->getSettingsRef().s3_max_single_part_upload_size);
    }
#endif
    else
    {
        throw Exception("Path: " + uri + " is illegal, only support write query result ve_tos", ErrorCodes::ILLEGAL_OUTPUT_PATH);
    }

    out_buf = wrapWriteBufferWithCompressionMethod(std::move(out_buf_raw), compression_method, compression_level);

    return &*out_buf;
}

void OutfileTarget::flushFile()
{
    out_buf->finalize();
}

OutfileTargetPtr OutfileTarget::getOutfileTarget(
    const std::string & uri,
    const std::string & format,
    const std::string & compression_method_str,
    int compression_level)
{
    return std::make_unique<OutfileTarget>(
        uri,
        format,
        compression_method_str,
        compression_level);
}

void OutfileTarget::setOufileCompression(
    const ASTQueryWithOutput * query_with_output, String & outfile_compression_method_str, UInt64 & outfile_compression_level)
{
    if (query_with_output->compression_method)
    {
        const auto & compression_method_node = query_with_output->compression_method->as<ASTLiteral &>();
        outfile_compression_method_str = compression_method_node.value.safeGet<std::string>();

        if (query_with_output->compression_level)
        {
            const auto & compression_level_node = query_with_output->compression_level->as<ASTLiteral &>();
            bool res = compression_level_node.value.tryGet<UInt64>(outfile_compression_level);
            if (!res)
            {
                throw Exception("Invalid compression level, must be in range or use default level without set", ErrorCodes::BAD_ARGUMENTS);
            }
        }
    }
}

bool OutfileTarget::checkOutfileWithTcpOnServer(const ContextMutablePtr & context)
{
    return context->getSettingsRef().outfile_in_server_with_tcp
        && context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY
        && context->getServerType() == ServerType::cnch_server;
}
}

