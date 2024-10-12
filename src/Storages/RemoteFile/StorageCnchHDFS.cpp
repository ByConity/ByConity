#include <common/logger_useful.h>
#include <Common/config.h>

#if USE_HDFS
#    include "StorageCnchHDFS.h"

#    include <filesystem>
#    include <utility>
#    include <DataStreams/RemoteBlockInputStream.h>
#    include <Formats/FormatFactory.h>
#    include <Storages/HDFS/HDFSCommon.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/InterpreterSelectQuery.h>
#    include <Interpreters/RequiredSourceColumnsVisitor.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Interpreters/predicateExpressionsUtils.h>
#    include <Interpreters/trySetVirtualWarehouse.h>
#    include <CloudServices/CnchServerResource.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/ASTSetQuery.h>
#    include <ServiceDiscovery/IServiceDiscovery.h>
#    include <Storages/AlterCommands.h>
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <Storages/RemoteFile/CnchFileSettings.h>
#    include <Storages/RemoteFile/StorageCloudHDFS.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/VirtualColumnUtils.h>
#    include <re2/re2.h>
#    include <re2/stringpiece.h>
#    include <Common/Exception.h>
#    include <Common/RemoteHostFilter.h>
#    include <Common/SettingsChanges.h>
#    include <Common/parseGlobs.h>
#    include <DataStreams/PartitionedBlockOutputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_NOT_FOUND;
}

FilePartInfos listWithRegexpMatching(
    const String & path_for_ls, const HDFSFSPtr & hdfsFS, const String & for_match, std::unordered_map<String, time_t> * last_mod_times)
{
    const size_t first_glob = for_match.find_first_of("*?{");

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
    const String suffix_with_globs = for_match.substr(end_of_path_without_globs); /// begin with '/'
    const String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'

    const size_t next_slash = suffix_with_globs.find('/', 1);
    re2::RE2 matcher(makeRegexpPatternFromGlobs(suffix_with_globs.substr(0, next_slash)));

    HDFSFileInfo ls;
    ls.file_info = hdfsListDirectory(hdfsFS.get(), prefix_without_globs.data(), &ls.length);
    if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
    {
        // ignore file not found exception, keep throw other exception, libhdfs3 doesn't have function to get exception type, so use errno.
        throw Exception(
            fmt::format("Cannot list directory {}: {}", prefix_without_globs, String(hdfsGetLastError())),
            ErrorCodes::BAD_GET); // todo(jiashuo):ACCESS_DENIED
    }
    FilePartInfos results;
    if (!ls.file_info && ls.length > 0)
        throw Exception("file_info shouldn't be null", ErrorCodes::LOGICAL_ERROR);
    for (int i = 0; i < ls.length; ++i)
    {
        const String full_path = String(ls.file_info[i].mName);
        const size_t last_slash = full_path.rfind('/');
        const String file_name = full_path.substr(last_slash);
        const bool looking_for_directory = next_slash != std::string::npos;
        const bool is_directory = ls.file_info[i].mKind == 'D';
        /// Condition with type of current file_info means what kind of path is it in current iteration of ls
        if (!is_directory && !looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                results.emplace_back(String(ls.file_info[i].mName), ls.file_info[i].mSize);
                if (last_mod_times)
                    (*last_mod_times)[results.back().name] = ls.file_info[i].mLastMod;
            }
        }
        else if (is_directory && looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                FilePartInfos result_part
                    = listWithRegexpMatching(std::filesystem::path(full_path) / "", hdfsFS, suffix_with_globs.substr(next_slash), last_mod_times);
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                std::move(result_part.begin(), result_part.end(), std::back_inserter(results));
            }
        }
    }

    return results;
}


FilePartInfos ListFilesWithGlobs(const ContextPtr & context, const FileURI & hdfs_uri, std::unordered_map<String, time_t> * last_mod_times)
{
    Poco::URI poco_uri(hdfs_uri.host_name);
    HDFSBuilderPtr builder = context->getGlobalContext()->getHdfsConnectionParams().createBuilder(poco_uri);
    HDFSFSPtr fs = createHDFSFS(builder.get());
    return listWithRegexpMatching("/", fs, hdfs_uri.file_path, last_mod_times);
}

FilePartInfos ListFiles(const ContextPtr & context, FilePartInfos & uris)
{
    Poco::URI poco_uri(HDFSURI(uris[0].name).host_name);
    HDFSBuilderPtr builder = context->getHdfsConnectionParams().createBuilder(poco_uri);
    auto fs = createHDFSFS(builder.get());
    FilePartInfos results;
    for (auto & uri : uris)
    {
        auto hdfs_uri = HDFSURI(uri.name);
        hdfsFileInfo * file_info = hdfsGetPathInfo(fs.get(), hdfs_uri.file_path.c_str());
        if (!file_info)
            throw Exception(ErrorCodes::FILE_NOT_FOUND, "File {} not exist", uri.name);
        uri.size = file_info->mSize;
        results.emplace_back(uri);
        hdfsFreeFileInfo(file_info, 1);
    }
    return results;
}

FilePartInfos StorageCnchHDFS::readFileList(ContextPtr query_context)
{
    if (arguments.is_glob_path)
        return ListFilesWithGlobs(query_context, FileURI(arguments.url), {});
    return ListFiles(query_context, file_list);
}

void StorageCnchHDFS::clear(ContextPtr query_context) {
    HDFSURI file(arguments.url);

    Poco::URI poco_uri(file.host_name);
    HDFSBuilderPtr builder = query_context->getHdfsConnectionParams().createBuilder(poco_uri);
    auto fs = createHDFSFS(builder.get());
    if (arguments.url.find(PartitionedBlockOutputStream::PARTITION_ID_WILDCARD) != String::npos)
    {
        // hdfsExists()=0 means exit
        if (hdfsExists(fs.get(), file.dir_path.c_str()))
        {
            LOG_TRACE(log, "Skip clear the {} not exist dir {}", getStorageID().getNameForLogs(), file.dir_path);
            return;
        }
        
        HDFSFileInfo ls;
        ls.file_info = hdfsListDirectory(fs.get(), file.dir_path.c_str(), &ls.length);
        if (!ls.file_info->mSize)
        {
            LOG_TRACE(log, "Skip clear the {} empty dir {}", getStorageID().getNameForLogs(), file.dir_path);
            return;
        }

        if (!hdfsDelete(fs.get(), file.dir_path.c_str(), true))
        {
            LOG_WARNING(log, "You now clear the {} dir {}", getStorageID().getNameForLogs(), file.dir_path);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to delete the {} dir {}, error: {}", getStorageID().getNameForLogs(), file.dir_path, hdfsGetLastError());
        }
    }
}

void StorageCnchHDFS::readByLocal(
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
    auto storage = StorageCloudHDFS::create(
        query_context,
        getStorageID(),
        storage_snapshot->metadata->getColumns(),
        storage_snapshot->metadata->getConstraints(),
        file_list,
        storage_snapshot->metadata->getSettingsChanges(),
        arguments,
        settings);
    storage->loadDataParts(parts);
    storage->read(query_plan, column_names, storage_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
}


BlockOutputStreamPtr StorageCnchHDFS::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    /// cnch table write only support server local
    return writeByLocal(query, metadata_snapshot, query_context);
}

BlockOutputStreamPtr StorageCnchHDFS::writeByLocal(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    auto storage = StorageCloudHDFS::create(query_context, getStorageID(), metadata_snapshot->getColumns(), metadata_snapshot->getConstraints(), file_list, metadata_snapshot->getSettingsChanges(), arguments, settings);
    auto streams = storage->write(query, metadata_snapshot, query_context);
    /// todo(jiashuo): insert new file and update the new file list in cache
    // file_list = storage->file_list;
    return streams;
}

void registerStorageCnchHDFS(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
    };

    factory.registerStorage("CnchHDFS", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 1 && engine_args.size() != 2 && engine_args.size() != 3)
            throw Exception(
                "Storage CnchHDFS requires exactly 1, 2 or 3 arguments: url, [format] and [compression].",
                ErrorCodes::BAD_ARGUMENTS);

        CnchFileArguments arguments;

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.getLocalContext());
        arguments.url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        if (engine_args.size() >= 2)
        {
            engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());
            arguments.format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        }

        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.getLocalContext());
            arguments.compression_method = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }
        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            arguments.partition_by = args.storage_def->partition_by->clone();

        CnchFileSettings settings = args.getContext()->getCnchFileSettings();
        settings.loadFromQuery(*args.storage_def);
        return StorageCnchHDFS::create(args.getContext(), args.table_id, args.columns, args.constraints, args.storage_def->settings->ptr(), arguments, settings);
    },
    features);
}

}
#endif
