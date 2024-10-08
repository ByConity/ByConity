#include "BNModelManager.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iterator>
#include <memory>
#include <bthread/mutex.h>
#include <string>
#include <utility>
#include <fcntl.h>
#include <stdio.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <ServiceDiscovery/ServiceDiscoveryCache.h>
#include <Storages/HDFS/HDFSFileSystem.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <bits/types/time_t.h>
#include <factorjoin/CardEstBNStringBuilder.h>
#include <fmt/core.h>
#if USE_HDFS
#include <libhdfs3/src/client/hdfs.h>
#include <libhdfs3/src/common/Logger.h>
#endif
#include <sys/stat.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace fs = std::filesystem;

String readFileFromLocal(fs::path path)
{
    std::ifstream f(path, std::ios::in | std::ios::binary);
    const auto sz = fs::file_size(path);
    String res(sz, '\0');
    f.read(res.data(), sz);
    return res;
}

#if USE_HDFS
namespace hdfs = HDFSCommon;
String readFileFromHdfs(const String & path)
{
    String file_path = path;
    /// url encoding is necessary otherwise `ReadBufferFromByteHDFS` will not recognize the filename
    boost::replace_all(file_path, "#", "%23");

    WriteBufferFromOwnString writer;
    std::shared_ptr<HDFSFileSystem> fs = getDefaultHdfsFileSystem();
    ReadBufferFromByteHDFS reader(file_path, fs->getHDFSParams());
    copyData(reader, writer);
    return writer.str();
}
#endif

BNModelManager::BNModelManager(
        ContextPtr global_context,
        BackgroundSchedulePool & schedule_pool_,
        const String & storage_type_,
        const String & base_directory_,
        int shard_num_,
        int fetch_interval_,
        int update_row_number_interval_,
        size_t model_max_size_bytes_,
        size_t max_size_bytes_)
    : WithContext(global_context), storage_type(storage_type_),
      shard_num(shard_num_),
      fetch_interval(fetch_interval_), update_row_number_interval(update_row_number_interval_),
      model_max_size_bytes(model_max_size_bytes_), max_size_bytes(max_size_bytes_)
{
    directory = base_directory_;
    fetch_model_task = schedule_pool_.createTask("BNModelManagerFetcher", [this]() {
        fetchImpl();
        fetch_model_task->scheduleAfter(fetch_interval);
    });

    if (update_row_number_interval > 0)
    {
        update_row_number_task = schedule_pool_.createTask("BNModelManagerUpdater", [this]() {
            updateRowNumbers();
            update_row_number_task->scheduleAfter(update_row_number_interval);
        });
    }
}

BNEstimator BNModelManager::getCardinalityEstimator(const StorageID & storage_id)
{
    auto it = models.find(storage_id);
    if (it == models.end())
        return BNEstimator();
    return it->second;
}

void BNModelManager::fetchImpl()
{
    Stopwatch watch;
    FetchStats stats;
#if USE_HDFS
    if (storage_type == "hdfs")
        stats = fetchHdfsImpl();
    else
#endif
        stats = fetchLocalImpl();
    LOG_INFO(
        getLogger("BNModelManagerFetcher"),
        "Fetching task runs in {} ms, scan total {} models, having {} reloaded models and {} exception",
        watch.elapsedMilliseconds(),
        stats.total,
        stats.reloaded,
        stats.exception);
}

BNModelManager::FetchStats BNModelManager::fetchLocalImpl(const StorageID & table_id)
{
    if (!fs::exists(directory))
    {
        LOG_WARNING(getLogger("BNModelManagerFetcher"), "Directory {} doesn't exists, skip this run", directory);
        return {};
    }
    int total = 0, reloaded = 0, exception = 0;
    for (const auto & entry : fs::directory_iterator(directory))
    {
        try
        {
            if (entry.is_directory())
                continue;
            auto file = entry.path().filename();
            auto ext = file.extension().string();
            if (ext != ".bifxml")
                continue;
            ++total;
            auto last_write_time = std::chrono::duration_cast<std::chrono::seconds>(entry.last_write_time().time_since_epoch()).count();
            auto storage_id = checkModelExpiredOrInvalid(file.string(), last_write_time);
            if (!storage_id || (table_id && storage_id != table_id))
                continue;

            /// Load the model
            auto xml_sz = fs::file_size(entry.path());
            auto meta_path = entry.path();
            meta_path.replace_extension(".json");
            auto json_sz = fs::file_size(meta_path);
            if (xml_sz + json_sz > model_max_size_bytes)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Model total size on disk is {} which exceeds the limit {}", xml_sz + json_sz, model_max_size_bytes);

            if (updateModel(storage_id, readFileFromLocal(entry.path()), readFileFromLocal(meta_path), last_write_time))
                ++reloaded;
            LOG_INFO(getLogger("BNModelManagerFetcher"), "Load BN Models: {}", storage_id.getFullNameNotQuoted());
        }
        catch (const Exception & e)
        {
            ++exception;
            LOG_WARNING(
                getLogger("BNModelManagerFetcher"),
                "Having exception <{}> while loading model {}, skip this run",
                e.what(),
                entry.path().string());
        }
        catch (const std::exception & e)
        {
            ++exception;
            LOG_WARNING(
                getLogger("BNModelManagerFetcher"),
                "Having exception <{}> while loading model {}, skip this run",
                e.what(),
                entry.path().string());
        }
        catch (...)
        {
            ++exception;
            LOG_WARNING(
                getLogger("BNModelManagerFetcher"),
                "Having unknown exception while loading model {}, skip this run",
                entry.path().string());
        }
    }

    return FetchStats{.total = total, .reloaded = reloaded, .exception = exception};
}

#if USE_HDFS
BNModelManager::FetchStats BNModelManager::fetchHdfsImpl(const StorageID & table_id)
{
    std::shared_ptr<HDFSFileSystem> hdfs_fs = getDefaultHdfsFileSystem();
    String shard_directory = fmt::format("{}/{}", directory, shard_num);
    if (shard_num != -1 && hdfs_fs->exists(shard_directory))
    {
        directory = shard_directory;
    }
    if (!hdfs_fs->exists(directory))
    {
        LOG_WARNING(getLogger("BNModelManagerFetcher"), "Directory {} doesn't exists, skip this run", directory);
        return {};
    }

    int total = 0, reloaded = 0, exception = 0;
    std::vector<String> files;
    hdfs_fs->list(directory, files);
    for (auto & file_name : files)
    {
        try
        {
            auto ext = file_name.substr(file_name.find_last_of('.'));
            if (ext != ".bifxml")
                continue;
            ++total;
            auto last_write_time = hdfs_fs->getLastModifiedInSeconds(fs::path(directory) / file_name);
            auto storage_id = checkModelExpiredOrInvalid(file_name, last_write_time);
            if (!storage_id || (table_id && storage_id != table_id))
                continue;

            /// Load the model
            auto xml_sz = hdfs_fs->getSize(fs::path(directory) / file_name);
            auto json_path = fs::path(directory) / file_name.replace(file_name.find_last_of('.') + 1, file_name.size(), "json");
            auto json_sz = hdfs_fs->getSize(json_path);
            if (xml_sz + json_sz > model_max_size_bytes)
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Model total size on disk is {} which exceeds the limit {}", xml_sz + json_sz, model_max_size_bytes);

            if (updateModel(storage_id, readFileFromHdfs(json_path.replace_extension(".bifxml")), readFileFromHdfs(json_path.replace_extension(".json")), last_write_time))
                ++reloaded;
            LOG_INFO(getLogger("BNModelManagerFetcher"), "Load BN Models: {}", storage_id.getFullNameNotQuoted());
        }
        catch (const Exception & e)
        {
            ++exception;
            LOG_WARNING(
                getLogger("BNModelManagerFetcher"),
                "Having exception <{}> while loading model {}, skip this run",
                e.what(),
                file_name);
        }
        catch (const std::exception & e)
        {
            ++exception;
            LOG_WARNING(
                getLogger("BNModelManagerFetcher"),
                "Having exception <{}> while loading model {}, skip this run",
                e.what(),
                file_name);
        }
        catch (...)
        {
            ++exception;
            LOG_WARNING(
                getLogger("BNModelManagerFetcher"), "Having unknown exception while loading model {}, skip this run", file_name);
        }
    }

    return FetchStats{.total = total, .reloaded = reloaded, .exception = exception};
}
#endif

StorageID BNModelManager::parseFromFilename(const String &filename)
{
    /// Expected filename format: {db_name}#{table_name}#{info}.{ext} while db_name and table_name can be
    /// quoted or non-quoted strings
    if (filename.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename is empty");
    String db_name, table_name;
    if (filename[0] == '`')
    {
        /// db_name is quoted
        auto db_close_quote = filename.find_first_of('`', 1);
        if (db_close_quote == std::string::npos || db_close_quote + 2 >= filename.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parse database or table name from file '{}'", filename);
        db_name = filename.substr(1, db_close_quote - 1);
        if (filename[db_close_quote+2] == '`')
        {
            /// table_name is quoted
            auto table_close_quote = filename.find_first_of('`', db_close_quote+3);
            if (table_close_quote == std::string::npos)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parse table name from file '{}'", filename);
            table_name = filename.substr(db_close_quote+3, table_close_quote-db_close_quote-3);
        }
        else
        {
            auto tok_first_pos = db_close_quote+1;
            if (filename[tok_first_pos] != '#')
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parse table name from file '{}'", filename);
            auto tok_second_pos = filename.find_first_of('#', tok_first_pos+1);
            table_name = filename.substr(tok_first_pos+1, tok_second_pos-tok_first_pos-1);
        }
    }
    else
    {
        auto tok_first_pos = filename.find_first_of('#');
        if (tok_first_pos == std::string::npos || tok_first_pos + 2 >= filename.size())
            return StorageID::createEmpty();
        db_name = filename.substr(0, tok_first_pos);
        if (filename[tok_first_pos+1] == '`')
        {
            /// table_name is quoted
            table_name = filename.substr(tok_first_pos+2, filename.find_first_of('`', tok_first_pos+2)-tok_first_pos-2);
        }
        else
        {
            auto tok_second_pos = filename.find_first_of('#', tok_first_pos+1);
            table_name = filename.substr(tok_first_pos+1, tok_second_pos-tok_first_pos-1);
        }
    }
    return StorageID(db_name, table_name);
}

StorageID BNModelManager::checkModelExpiredOrInvalid(const String & filename, time_t last_write_time)
{
    StorageID storage_id = parseFromFilename(filename);
    if (!storage_id || !DatabaseCatalog::instance().isTableExist(storage_id, getContext()))
        return StorageID::createEmpty();

    std::lock_guard lk{insert_mutex};
    auto it = models.find(storage_id);
    if (it != models.end() && it->second.ts >= last_write_time)
        return StorageID::createEmpty();
    return storage_id;
}

bool BNModelManager::updateModel(const StorageID & storage_id, const String & model_xml, const String & model_json, time_t last_write_time)
{
    /// remove the suffix "_local" from the table_name
    ml4cardest::CardEstBNStringBuilder builder(fmt::format("{}.{}", storage_id.database_name, storage_id.table_name.substr(0, storage_id.table_name.size() - 6)));
    builder.loadEstimator(model_xml, model_json);
    auto new_model = ml4cardest::CardEstBNPtr(builder.getEstimator());
    auto model_sz = model_xml.size() + model_json.size(); /// TODO @hanyuxing: get accurate the model size in-memory
    /// init inference context for model
    new_model->initInferenceContext();
    std::lock_guard lk{insert_mutex};
    auto it = models.find(storage_id);
    if (it != models.end() && it->second.ts >= last_write_time)
        return false;
    if (it == models.end())
    {
        if (total_size_bytes + model_sz > max_size_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Total model size reach limit {} / {}, cannot add new model with size {}", total_size_bytes, max_size_bytes, model_sz);
        total_size_bytes += model_sz;
        models.emplace(storage_id, BNEstimator(last_write_time, model_sz, std::move(new_model)));
    }
    else
    {
        auto old_model_sz = it->second.bytes;
        if (total_size_bytes + model_sz - old_model_sz > max_size_bytes)
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Total model size reach limit {} / {}, cannot update model with exceed size {}", total_size_bytes, max_size_bytes, model_sz);
        total_size_bytes += model_sz - old_model_sz;
        it->second.model.reset();
        it->second.model = std::move(new_model);
        it->second.ts = last_write_time;
        it->second.bytes = model_sz;
    }
    return true;
}

void BNModelManager::updateRowNumbers()
{
    for (auto & model: models)
    {
        auto storage_ptr = DatabaseCatalog::instance().tryGetTable(model.first, getContext());
        auto storage_merge_tree =  std::dynamic_pointer_cast<const MergeTreeData>(storage_ptr);

        model.second.row_number = 0;
        if (storage_merge_tree)
        {
            auto parts = storage_merge_tree->getDataPartsVector();

            for (const auto & part : parts)
                model.second.row_number += part->rows_count;

            LOG_INFO(getLogger("BNModelManagerUpdater"), "Update row number of table {}: {}",
                     model.first.getFullNameNotQuoted(), model.second.row_number);
        }

    }
}

void BNModelManager::fetchSync(const StorageID & table_id)
{
    auto start = std::chrono::high_resolution_clock::now();
    FetchStats stats;
#if USE_HDFS
    if (storage_type == "hdfs")
        stats = fetchHdfsImpl(table_id);
    else
#endif
        stats = fetchLocalImpl(table_id);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG_INFO(
        getLogger("BNModelManagerFetcher"), "Fetch task run in {} ms, reloaded: {}", duration.count(), stats.reloaded);
}

}
