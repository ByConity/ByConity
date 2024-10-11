#include <Common/config.h>

#if USE_HDFS
#    include "StorageCloudHDFS.h"

#    include <filesystem>
#    include <utility>
#    include <DataStreams/IBlockInputStream.h>
#    include <DataStreams/IBlockOutputStream.h>
#    include <Storages/HDFS/HDFSCommon.h>
#    include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#    include <Storages/HDFS/WriteBufferFromHDFS.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTInsertQuery.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/StorageFactory.h>
#    include <re2/stringpiece.h>
#    include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ACCESS_DENIED;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

StorageCloudHDFS::FileBufferClient::FileBufferClient(const ContextPtr & query_context_, const String & hdfs_file_) : query_context(query_context_)
{
    FileURI hdfs_uri(hdfs_file_);
    Poco::URI poco_uri(hdfs_uri.host_name);
    HDFSBuilderPtr builder = query_context->getGlobalContext()->getHdfsConnectionParams().createBuilder(poco_uri);
    fs = createHDFSFS(builder.get());
}

std::unique_ptr<ReadBuffer> StorageCloudHDFS::FileBufferClient::createReadBuffer(const DB::String & file)
{
    ReadSettings settings;
    settings.byte_hdfs_pread = true;
    return std::make_unique<ReadBufferFromByteHDFS>(file, query_context->getGlobalContext()->getHdfsConnectionParams(), settings);
}

std::unique_ptr<WriteBuffer> StorageCloudHDFS::FileBufferClient::createWriteBuffer(const DB::String & file)
{
    return std::make_unique<WriteBufferFromHDFS>(file, query_context->getHdfsConnectionParams(), DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY, query_context->getSettingsRef().overwrite_current_file);
}

bool StorageCloudHDFS::FileBufferClient::exist(const DB::String & file)
{
    return !hdfsExists(fs.get(), file.c_str());
}

void registerStorageCloudHDFS(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
    };

    factory.registerStorage("CloudHDFS", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5)
            throw Exception(
                "Storage CloudHDFS requires exactly 5 arguments on worker: database_name, table_name, url, format and compression.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            arguments.partition_by = args.storage_def->partition_by->clone();

        CnchFileSettings settings = args.getContext()->getCnchFileSettings();
        settings.loadFromQuery(*args.storage_def);
        LOG_TRACE(
            getLogger("StorageCloudHDFS"),
            fmt::format(
                "create cloud hdfs table: database={}, table={}, url={}, format={}, compression={}",
                database,
                table,
                arguments.url,
                arguments.format_name,
                arguments.compression_method));

        Strings files{arguments.url};
        return StorageCloudHDFS::create(args.getContext(), args.table_id, args.columns, args.constraints, files, args.storage_def->settings->ptr(), arguments, settings);
    },
    features);
}

}
#endif
