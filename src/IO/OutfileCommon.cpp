#include <Common/config.h>

#include <IO/OutfileCommon.h>
#include <IO/WriteBuffer.h>

#include <IO/CompressionMethod.h>
#include <IO/HTTPSender.h>
#include <IO/OSSCommon.h>
#include <IO/VETosCommon.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <ServiceDiscovery/ServiceDiscoveryConsul.h>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Common/config.h>
#include "Interpreters/Context_fwd.h"

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>

#include <string>

#if USE_AWS_S3
#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromS3.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_OUTPUT_PATH;
    extern const int INTO_OUTFILE_NOT_ALLOWED;
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
}

constexpr UInt64 DEFAULT_SPLIT_FILE_SIZE_IN_MB = 256UL;  // Default threshold size when splitting out file
constexpr UInt64 MAX_SPLIT_FILE_SIZE_IN_MB = 3096UL; // Upper bound of the threshold for split data

size_t calcFileSizeLimit(ContextMutablePtr & context)
{
    size_t split_file_size_in_mb = context->getSettingsRef().split_file_size_in_mb;
    size_t split_file_limit = split_file_size_in_mb * 1024 * 1024;
    // Considering that split_size is actually used in memory, in which
    // data size is normally less than that in disk, thus 3GB maybe
    // a quite large limit. Here it is treated as an undesirable threshold.
    if (split_file_size_in_mb == 0 || split_file_size_in_mb > MAX_SPLIT_FILE_SIZE_IN_MB)
        split_file_limit = DEFAULT_SPLIT_FILE_SIZE_IN_MB * 1024 * 1024;

    return split_file_limit;
}

String getFullOutPath(String & format_name, String & path, int serial_no, CompressionMethod compression_method)
{
    const static std::vector<std::pair<String, String>> format_suffixes
        = {{"JSON", ".json"}, {"CSV", ".csv"}, {"Tab", ".tsv"}, {"TSV", ".tsv"}, {"Parquet", ".parquet"}};

    String out_path = path + "clickhouse_outfile_" + std::to_string(serial_no);
    for (const auto & suffix : format_suffixes)
    {
        if (startsWith(format_name, suffix.first))
        {
            out_path += suffix.second;
            break;
        }
    }

    if (compression_method != CompressionMethod::None)
        out_path += "." + getFileSuffix(compression_method);

    return out_path;
}

void OutfileTarget::updateBaseFilePathIfDistributedOutput()
{
    if (context->getSettingsRef().enable_distributed_output)
    {
        /// each worker have to use different file name if concurrent writing
        auto file_prefix = getIPOrFQDNOrHostName() + "_" + std::to_string(context->getTCPPort()) + "_";
        std::replace(file_prefix.begin(), file_prefix.end(), ':', '-');

        Poco::Path file_path(request_uri);
        if (file_path.isFile())
        {
            String directory_path = request_uri.substr(0, request_uri.find_last_of('/'));
            converted_uri = std::filesystem::path(directory_path) / (file_prefix + file_path.getFileName());
        }
        else
        {
            converted_uri = std::filesystem::path(request_uri) / file_prefix;
        }
    }
    else
    {
        converted_uri = request_uri;
    }
}

OutfileTarget::OutfileTarget(
    ContextMutablePtr context_, std::string uri_, std::string format_, std::string compression_method_str_, int compression_level_)
    : context(context_)
    , request_uri(uri_)
    , format(format_)
    , compression_method_str(compression_method_str_)
    , compression_level(compression_level_)
{
    const Poco::URI out_uri(request_uri);
    scheme = out_uri.getScheme();
    compression_method = chooseCompressionMethod(request_uri, compression_method_str);

    // hdfs always use CompressionMethod::Gzip default
    if (scheme == "hdfs" && compression_method_str.empty())
        compression_method = CompressionMethod::Gzip;
    // lasfs maybe not support compression
    else if (scheme == "lasfs")
        compression_method = CompressionMethod::None;

    updateBaseFilePathIfDistributedOutput();
    // we have to use raw request uri to judge file or directory
    if (Poco::Path(request_uri).isFile())
    {
        out_type = OutType::SINGLE_OUT_FILE;
        real_outfile_path = converted_uri;
    }
    else
    {
        out_type = OutType::MULTI_OUT_FILE;
        split_file_limit = calcFileSizeLimit(context);
        /// the first real out_file_path;
        real_outfile_path = getFullOutPath(format, converted_uri, serial_no, compression_method);
    }
}

void OutfileTarget::getRawBuffer()
{
    const Poco::URI out_uri(real_outfile_path);

    if (scheme.empty())
    {
        out_buf_raw = std::make_unique<WriteBufferFromFile>(real_outfile_path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
    }
    else if (scheme == "tos")
    {
        if (out_uri.getQueryParameters().empty())
            throw Exception("Missing access key, please check configuration.", ErrorCodes::BAD_ARGUMENTS);
        if (compression_method != CompressionMethod::None)
            throw Exception("Compression is not supported for tos outfile", ErrorCodes::BAD_ARGUMENTS);

        out_buf_raw = std::make_unique<WriteBufferFromOwnString>();
    }
#if USE_HDFS
    else if (DB::isHdfsOrCfsScheme(scheme))
    {
        out_buf_raw = std::make_unique<WriteBufferFromHDFS>(
            real_outfile_path,
            context->getHdfsConnectionParams(),
            context->getSettingsRef().max_hdfs_write_buffer_size,
            O_WRONLY,
            context->getSettingsRef().overwrite_current_file);
    }
#endif
#if USE_LASFS
    else if (scheme == "lasfs")
    {
        LasfsSettings lasfs_settings;
        refreshCurrentLasfsSettings(lasfs_settings, context);
        out_buf_raw = std::make_unique<WriteBufferFromLasfs>(lasfs_settings, "lasfs:/" + out_uri.getHost() + out_uri.getPath());
    }
#endif
#if USE_AWS_S3
    else if (scheme == "vetos" || scheme == "oss" || isS3URIScheme(scheme))
    {
        std::optional<S3::S3Config> s3_config;
        String key;
        if (scheme == "vetos")
        {
            VETosConnectionParams vetos_connect_params = VETosConnectionParams::getVETosSettingsFromContext(context);
            auto tos_uri = verifyTosURI(real_outfile_path);
            std::string bucket = tos_uri.getHost();
            key = getTosKeyFromURI(tos_uri);
            s3_config = VETosConnectionParams::getS3Config(vetos_connect_params, bucket);
        }
        else if (scheme == "oss")
        {
            OSSConnectionParams oss_connect_params = OSSConnectionParams::getOSSSettingsFromContext(context);
            auto oss_uri = verifyOSSURI(real_outfile_path);
            std::string bucket = oss_uri.getHost();
            key = getOSSKeyFromURI(oss_uri);
            s3_config = OSSConnectionParams::getS3Config(oss_connect_params, bucket);
        }
        else
        {
            S3::URI s3_uri(out_uri);
            String endpoint = s3_uri.endpoint.empty() ? context->getSettingsRef().s3_endpoint.toString() : s3_uri.endpoint;
            s3_config = S3::S3Config(
                endpoint,
                context->getSettingsRef().s3_region.toString(),
                s3_uri.bucket,
                context->getSettingsRef().s3_ak_id,
                context->getSettingsRef().s3_ak_secret,
                "",
                "",
                context->getSettingsRef().s3_use_virtual_hosted_style);
            key = s3_uri.key;
        }
        out_buf_raw = std::make_unique<WriteBufferFromS3>(
            s3_config->create(),
            s3_config->bucket,
            key,
            context->getSettingsRef().s3_min_upload_part_size,
            context->getSettingsRef().s3_max_single_part_upload_size,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            true,
            context->getSettingsRef().s3_use_parallel_upload,
            context->getSettingsRef().s3_parallel_upload_pool_size);
    }
#endif
    else
    {
        throw Exception("Path: " + real_outfile_path + " is illegal, scheme " + scheme + " is not supported.", ErrorCodes::ILLEGAL_OUTPUT_PATH);
    }
}

std::shared_ptr<WriteBuffer> OutfileTarget::getOutfileBuffer(bool allow_into_local)
{
    if (scheme.empty() && !allow_into_local)
        throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);
    // get raw write buffer
    getRawBuffer();
    // wrap raw write buffer with compression buffer
    out_buf = wrapWriteBufferWithCompressionMethod(std::move(out_buf_raw), compression_method, compression_level);

    return out_buf;
}

WriteBuffer * OutfileTarget::updateBuffer()
{
    WriteBuffer * new_buffer;
    if (compression_method == CompressionMethod::None)
    {
        new_buffer = out_buf->inplaceReconstruct(real_outfile_path, nullptr);
    }
    else
    {
        // get raw write buffer
        getRawBuffer();
        // wrap raw write buffer with compression buffer
        new_buffer = out_buf->inplaceReconstruct(real_outfile_path, std::move(out_buf_raw));
    }
    return new_buffer;
}

void OutfileTarget::flushFile()
{
    if (auto * out_tos_buf = dynamic_cast<WriteBufferFromOwnString *>(out_buf.get()))
    {
        try
        {
            const Poco::URI out_uri(request_uri);
            std::string host = out_uri.getHost();
            auto port = out_uri.getPort();

            if (host.empty() || port == 0)
            {
                // choose a tos server randomly
                ServiceDiscoveryClientPtr service_discovery = std::make_shared<ServiceDiscoveryConsul>(context->getConfigRef());
                ServiceEndpoints tos_servers = service_discovery->lookupEndpoints(TOS_PSM);
                if (tos_servers.empty())
                    throw Exception("Can not find tos servers with PSM: " + String(TOS_PSM), ErrorCodes::NETWORK_ERROR);

                auto generator = std::mt19937(std::random_device{}());
                std::uniform_int_distribution<size_t> distribution(0, tos_servers.size() - 1);
                ServiceEndpoint tos_server = tos_servers.at(distribution(generator));
                host = tos_server.host;
                port = tos_server.port;
            }

            String tos_server = std::get<0>(safeNormalizeHost(host)) + ":" + std::to_string(port);

            Poco::URI tos_uri("http://" + tos_server + out_uri.getPath());
            String access_key = out_uri.getQueryParameters().at(0).second;
            HttpHeaders http_headers{{"X-Tos-Access", access_key}};

            const Settings & settings = context->getSettingsRef();
            ConnectionTimeouts timeouts(settings.http_connection_timeout, settings.http_send_timeout, settings.http_receive_timeout);

            HTTPSender http_sender(tos_uri, Poco::Net::HTTPRequest::HTTP_PUT, timeouts, http_headers);
            String res = (*out_tos_buf).str();
            if (res.empty())
                res = "\n";
            http_sender.send(res);
            http_sender.handleResponse();
        }
        catch (...)
        {
            out_tos_buf->finalize();
            throw;
        }
    }
    else
    {
        out_buf->finalize();
    }
}

void OutfileTarget::updateOutPathIfNeeded()
{
    if (out_type == MULTI_OUT_FILE)
    {
        ++serial_no;
        current_bytes = 0;
        real_outfile_path = getFullOutPath(format, converted_uri, serial_no, compression_method);
    }
}

void OutfileTarget::setOutfileCompression(
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

// If only outfile_in_server_with_tcp is specified, outfile is executed in server side;
// If only enable_distributed_output is specified, worker will be considered as server role,
// and outfile is executed in worker side.
// If both is specified, outfile is executed only in worker side.
bool OutfileTarget::checkOutfileWithTcpOnServer(ContextMutablePtr & context)
{
    return (context->getServerType() == ServerType::cnch_server
            && context->getSettingsRef().outfile_in_server_with_tcp
            && !context->getSettingsRef().enable_distributed_output
            && context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        || (context->getSettingsRef().enable_distributed_output && context->getServerType() == ServerType::cnch_worker);
}

void OutfileTarget::toProto(Protos::OutfileWriteStep::OutfileTarget & proto, bool) const
{
    proto.set_request_uri(request_uri);
    proto.set_format(format);
    proto.set_compression_method_str(compression_method_str);
    proto.set_compression_level(compression_level);
}

std::shared_ptr<OutfileTarget> OutfileTarget::fromProto(const Protos::OutfileWriteStep::OutfileTarget & proto, ContextPtr context)
{
    return std::make_shared<OutfileTarget>(
        Context::createCopy(context), proto.request_uri(), proto.format(), proto.compression_method_str(), proto.compression_level());
}
}
