#include "DumpHelper.h"
#include <optional>
#include <algorithm>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/Settings.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/ITableFunction.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/S3ObjectMetadata.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>
#include <Disks/registerDisks.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/DiskLocal.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Poco/String.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/HelpFormatter.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadStatus.h>
#include <Common/escapeForFileName.h>
// #include <Common/getFQDNOrHostName.h>
#include <common/ErrorHandlers.h>
#include <Common/Macros.h>
#include <common/scope_guard.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int SYNTAX_ERROR;
    extern const int INVALID_PARTITION_VALUE;
    extern const int METADATA_MISMATCH;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int TOO_MANY_PARTS;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int CANNOT_UPDATE_COLUMN;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_MREMAP;
    extern const int BAD_TTL_EXPRESSION;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int UNKNOWN_TABLE;
    extern const int NO_SUCH_DATA_PART;
    extern const int BAD_DATA_PART_NAME;
}

class ClickHouseDumper : public Poco::Util::Application
{
public:
    using Poco::Util::Application::Application;
    using CanDumpCallback = std::function<bool(String partition_id)>;

    ~ClickHouseDumper() override;

    int main(const std::vector<String> & args) override;
    void initialize(Poco::Util::Application & self) override;
    void defineOptions(Poco::Util::OptionSet & options) override;

    void handleHelp(const String &, const String &);
    void handlePartition(const String &, const String &);
    void handlePartitionlist(const String &, const String &);
    void handleSkipPartitionlist(const String &, const String &);

    void removeConfiguration(Poco::Util::LayeredConfiguration& cfg,
        const String& prefix);
    void initPath();
    void initHDFS();

    StoragePtr createStorageFromCreateQuery(
        const String & create_table_query,
        const String & database,
        const String & table,
        Context & context,
        bool & is_atomic_database);
    void processDatabase(const String & database);
    void processTable(const String & database, const String & table,
                      const String & partition,
                      const std::vector<String>& partitionlist,
                      const std::vector<String>& skippartitionlist);

    void initDataDiskPath(const String & escaped_database,
                          const String & escaped_table,
                          std::vector<String> & data_paths,
                          bool is_multi_disk);

    void startLoadAndDumpPart(StorageCloudMergeTree & cloud,
                              const ActiveDataPartSet & part_names,
                              Snapshot & snapshot,
                              const std::shared_ptr<IDisk> & local_disk,
                              const std::shared_ptr<IDisk> & remote_disk);

    void getUniqueTableActivePartsFromDisk(StorageCloudMergeTree & cloud,
                                           Snapshot & snapshot,
                                           const String & escaped_database,
                                           const String & escaped_table,
                                           std::vector<String> & data_paths,
                                           std::map<String, ActiveDataPartSet> & active_parts_multi_disk,
                                           bool is_multi_disk,
                                           CanDumpCallback can_dump);

    void loadAndDumpPart(StorageCloudMergeTree & cloud,
                         MergeTreeCNCHDataDumper & dumper,
                         const String & part_name,
                         const UInt64 version,
                         const std::shared_ptr<IDisk> & local_disk,
                         const std::shared_ptr<IDisk> & remote_disk);

private:
    SharedContextHolder shared_context;
    ContextMutablePtr global_context;
    Settings settings;
    Int64 current_shard_number {0};
    LoggerPtr log{};
    UniqueTableDumpHelper unique_table_dump_helper;
};

ClickHouseDumper::~ClickHouseDumper()
{
    if (global_context)
        global_context->shutdown();
}

void ClickHouseDumper::initialize(Poco::Util::Application & self)
{
    Poco::Util::Application::initialize(self);

    // Turn off server logging to stderr
    // if (!config().has("verbose"))
    // {
    //     Poco::Logger::root().setLevel("none");
    //     Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    // }
}

void ClickHouseDumper::defineOptions(Poco::Util::OptionSet & options)
{
    Poco::Util::Application::defineOptions(options);
    using Me = std::decay_t<decltype(*this)>;


    options.addOption(Poco::Util::Option("config-file", "C", "load configuration from a given file") //
                          .required(false)
                          .argument("<file>")
                          .binding("config-file"));

    options.addOption(Poco::Util::Option("database", "", "database to be dumped") //
                          .required(true)
                          .argument("<database>")
                          .binding("database"));

    options.addOption(Poco::Util::Option("table", "", "table to be dumped") //
                          .required(false)
                          .argument("<table>")
                          .binding("table"));

    options.addOption(Poco::Util::Option("partition", "", "partition to be dumped") //
                          .required(false)
                          .argument("<partition>")
                          .binding("partition")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handlePartition)));

    options.addOption(Poco::Util::Option("partition_list", "", "multi-partitions to be dumped") //
                          .required(false)
                          .argument("<partition_list>")
                          .binding("partition_list")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handlePartitionlist)));

    options.addOption(Poco::Util::Option("skip_partition_list", "", "skip-partitions to be dumped") //
                          .required(false)
                          .argument("<skip_partition_list>")
                          .binding("skip_partition_list")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handleSkipPartitionlist)));

    options.addOption(Poco::Util::Option("hdfs_nnproxy", "", "") //
                          .required(true)
                          .argument("<psm>")
                          .binding("output_hdfs_nnproxy"));

    options.addOption(Poco::Util::Option("output", "O", "output path on shared-storage, the final part path is under output/database/table/") //
                          .required(true)
                          .argument("<path>")
                          .binding("output"));

    options.addOption(Poco::Util::Option("parallel", "P", "threads for dumping parts") //
                          .required(false)
                          .argument("<num_threads>")
                          .binding("parallel"));

    options.addOption(Poco::Util::Option("overwrite", "R", "overwrite existed parts") //
                          .required(false)
                          .binding("overwrite"));

    options.addOption(Poco::Util::Option("skip_corrupt_parts", "S", "skip corrupt parts") //
                          .required(false)
                          .binding("skip_corrupt_parts"));

    options.addOption(Poco::Util::Option("skip_unkowning_settings", "", "skip dumper unknown settings") //
                          .required(false)
                          .binding("skip_unkowning_settings"));

    options.addOption(Poco::Util::Option("multi_disk_path_list", "", "multi disk path list") //
                          .required(false)
                          .argument("<multi_disk_path_list>")
                          .binding("multi_disk_path_list"));

    using Me = std::decay_t<decltype(*this)>;
    options.addOption(Poco::Util::Option("help", "", "produce this help message") //
                          .binding("help")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handleHelp)));
}

void ClickHouseDumper::handleHelp(const String &, const String &)
{
    Poco::Util::HelpFormatter helpFormatter(options());

    String command_name = commandName();
    if (!endsWith(command_name, "dumper"))
        command_name += " dumper";
    helpFormatter.setCommand(command_name);

    helpFormatter.setHeader("Dump tables from internal ClickHouse to shared-storage");
    helpFormatter.setUsage("--config-file <config-file> --database <db> --table <table> --partition <partition> --output <path>");
    helpFormatter.format(std::cerr);

    stopOptionsProcessing();
}

void ClickHouseDumper::handlePartition(const String &, const String &)
{
    String partitionlist = config().getString("partition_list","");
    String skip_partition_list =  config().getString("skip_partition_list","");

    if(!partitionlist.empty() || !skip_partition_list.empty())
    {
        stopOptionsProcessing();
        throw Poco::InvalidArgumentException("argument to dump exist parameter conflict");
    }
}

void ClickHouseDumper::handlePartitionlist(const String &, const String &)
{
    String partition = config().getString("partition","");
    String skip_partition_list =  config().getString("skip_partition_list","");

    if(!partition.empty() || !skip_partition_list.empty())
    {
        stopOptionsProcessing();
        throw Poco::InvalidArgumentException("argument to dump exist parameter conflict");
    }
}

void ClickHouseDumper::handleSkipPartitionlist(const String &, const String &)
{
    String partition = config().getString("partition","");
    String partition_list =  config().getString("partition_list","");

    if(!partition.empty() || !partition_list.empty())
    {
        stopOptionsProcessing();
        throw Poco::InvalidArgumentException("argument to dump exist parameter conflict");
    }
}

StoragePtr ClickHouseDumper::createStorageFromCreateQuery(
    const String & create_table_query, const String & database, const String & table, Context & context, bool & is_atomic_database)
{
    ParserCreateQuery p_create_query;
    auto ast = parseQuery(p_create_query, create_table_query, global_context->getSettingsRef().max_query_size, global_context->getSettingsRef().max_parser_depth);

    auto & create_query = ast->as<ASTCreateQuery &>();
    if (String::npos == create_query.storage->engine->name.find("MergeTree"))
        return {};

    if (String::npos != create_query.storage->engine->name.find("HaUnique"))
        unique_table_dump_helper.parseZKReplicaPathFromEngineArgs(create_query.storage, database, table, context);

    create_query.database = std::move(database);
    create_query.attach = true;
    UUID uuid = create_query.uuid;
    if (create_query.table == "_" && (uuid != UUIDHelpers::Nil))
    {
        is_atomic_database = true;
        create_query.table = std::move(table);
    }

    if (uuid == UUIDHelpers::Nil)
        create_query.uuid = UUIDHelpers::generateV4();

    auto engine = std::make_shared<ASTFunction>();
    engine->name = "CloudMergeTree";
    engine->arguments = std::make_shared<ASTExpressionList>();
    engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.database));
    engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.table));
    create_query.storage->set(create_query.storage->engine, engine);

    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*create_query.columns_list->columns, global_context, create_query.attach);
    ConstraintsDescription constraints = InterpreterCreateQuery::getConstraintsDescription(create_query.columns_list->constraints);
    ForeignKeysDescription foreign_keys = InterpreterCreateQuery::getForeignKeysDescription(create_query.columns_list->foreign_keys);
    UniqueNotEnforcedDescription unique = InterpreterCreateQuery::getUniqueNotEnforcedDescription(create_query.columns_list->unique);
    bool skip_unknown_settings = config().has("skip_unkowning_settings");

    return StorageFactory::instance().get(
        create_query,
        "",
        Context::createCopy(global_context),
        global_context,
        columns,
        constraints,
        foreign_keys,
        unique,
        false /*has_force_restore_data_flag*/,
        nullptr,
        skip_unknown_settings);
}

void ClickHouseDumper::removeConfiguration(Poco::Util::LayeredConfiguration& cfg, const String& prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    cfg.keys(prefix, keys);
    for (const String& key : keys)
    {
        removeConfiguration(cfg, prefix + "." + key);
    }
    cfg.remove(prefix);
}

void ClickHouseDumper::initPath()
{
    Poco::Util::LayeredConfiguration& cfg = config();

    /// Setup path
    String path = cfg.getString("path", ".");
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("Empty root path", ErrorCodes::BAD_ARGUMENTS);
    if (path.back() != '/')
        path += '/';

    LOG_DEBUG(log, "ClickHouse default set data path {}", path);

    global_context->setPath(path);

    /// In case of empty path set paths to helpful directories
    String cd = Poco::Path::current();
    // context->setTemporaryPath(cd + "tmp");
    global_context->setFlagsPath(cd + "flags");
    global_context->setUserFilesPath(""); // user's files are everywhere

    // Parse default local disk path
    String default_local_disk_path = path;
    String default_hdfs_disk_path = cfg.getString("output") + "/";

    // Reset default storage policy configuration
    removeConfiguration(cfg, "storage_configuration");

    // Set default storage policy
    {
        cfg.setString("storage_configuration", "");
        cfg.setString("storage_configuration.disks.default.path", "");
        cfg.setString("storage_configuration.disks.default_hdfs.path",
            default_hdfs_disk_path);
        cfg.setString("storage_configuration.disks.default_hdfs.type", "hdfs");
        String default_volume_cfg_prefix = "storage_configuration.policies.default.volumes";
        cfg.setString(default_volume_cfg_prefix + ".local.default", "default");
        cfg.setString(default_volume_cfg_prefix + ".local.disk", "default");
        cfg.setString(default_volume_cfg_prefix + ".hdfs.default", "default_hdfs");
        cfg.setString(default_volume_cfg_prefix + ".hdfs.disk", "default_hdfs");
    }

    /// Debug logging
    LOG_DEBUG(log, "ClickHouse default data path {}", default_local_disk_path);
}

void ClickHouseDumper::initHDFS()
{
    /// Init HDFS3 client config path
    String hdfs_config = config().getString("hdfs3_config", "");
    if (!hdfs_config.empty())
    {
        setenv("LIBHDFS3_CONF", hdfs_config.c_str(), 1);
    }

    /// Options load from command line argument use priority -100 in layeredconfiguration, so construct
    /// hdfs params from config directly rather than from config file
    HDFSConnectionParams hdfs_params = HDFSConnectionParams::parseFromMisusedNNProxyStr(
        config().getString("output_hdfs_nnproxy", "nnproxy"), config().getString("hdfs_user", "clickhouse"));
    global_context->setHdfsConnectionParams(hdfs_params);
    /// register default hdfs file system
    bool has_hdfs_disk = false;
    for (const auto & [name, disk] : global_context->getDisksMap())
    {
        if (disk->getType() == DiskType::Type::ByteHDFS)
        {
            has_hdfs_disk = true;
        }
    }

    if (has_hdfs_disk)
    {
        const int hdfs_max_fd_num = config().getInt("hdfs_max_fd_num", 100000);
        const int hdfs_skip_fd_num = config().getInt("hdfs_skip_fd_num", 100);
        const int hdfs_io_error_num_to_reconnect = config().getInt("hdfs_io_error_num_to_reconnect", 10);
        registerDefaultHdfsFileSystem(hdfs_params, hdfs_max_fd_num, hdfs_skip_fd_num, hdfs_io_error_num_to_reconnect);
    }
}

void ClickHouseDumper::processDatabase(const String & database)
{
    String escaped_database = escapeForFileName(database);

    String db_metadata_path = config().getString("path") + "/metadata/" + escaped_database + "/";
    for (Poco::DirectoryIterator it(db_metadata_path); it != Poco::DirectoryIterator(); ++it)
    {
        auto & name = it.name();
        if (!endsWith(name, ".sql"))
            continue;

        try
        {
            processTable(database, unescapeForFileName(name), "",std::vector<String>(),std::vector<String>());
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void ClickHouseDumper::initDataDiskPath(
    const String & escaped_database,
    const String & escaped_table,
    std::vector<String> & data_paths,
    bool is_multi_disk)
{
    if (!is_multi_disk)
    {
        String data_path = config().getString("path") + "/data/" +  escaped_database + "/" + escaped_table;
        data_paths.push_back(data_path);

        return ;
    }

    String fuzzy_disk_names =  config().getString("multi_disk_path_list");
    std::vector<String> fuzzy_disk_lists = parseDescription(fuzzy_disk_names, 0, fuzzy_disk_names.length(), ',' , 100/* hard coded max files */);
    std::vector<Strings> disks_list;
    for (auto fuzzy_name : fuzzy_disk_lists)
    {
        disks_list.push_back(parseDescription(fuzzy_name, 0, fuzzy_name.length(), '|', 100));
    }

    for (const auto & vec_names : disks_list)
    {
        for (const auto & disk_name : vec_names)
        {
            String disk_path = disk_name + "data/" + escaped_database + "/" + escaped_table + "/";

            data_paths.push_back(std::move(disk_path));
        }
    }
}

void ClickHouseDumper::getUniqueTableActivePartsFromDisk(
    StorageCloudMergeTree & cloud,
    Snapshot & snapshot,
    const String & escaped_database,
    const String & escaped_table,
    std::vector<String> & data_paths,
    std::map<String, ActiveDataPartSet> & active_parts_multi_disk,
    bool is_multi_disk,
    CanDumpCallback can_dump)
{
    ActiveDataPartSet part_names(cloud.format_version);
    for (auto it = snapshot.begin(); it != snapshot.end(); it++)
    {
        MergeTreePartInfo part_info;
        if (!MergeTreePartInfo::tryParsePartName(it->first, &part_info, cloud.format_version))
            throw Exception("Can't parse part name: " + it->first + " in Snapshot. ", ErrorCodes::BAD_DATA_PART_NAME);

        if (!can_dump(part_info.partition_id))
            continue;

        if (is_multi_disk)
        {
            bool flag = false;
            for (const auto & data_path : data_paths)
            {
                if (Poco::File(data_path + it->first).exists())
                {
                    part_names.add(it->first);
                    flag = true;
                    break;
                }
            }

            if (!flag)
                throw Exception(
                    "Part: " + it->first + " in snapshot doesn't exist in dir , please check and retry. ",
                    ErrorCodes::NO_SUCH_DATA_PART);
        }
        else
        {
            DiskPtr local_disk = global_context->getStoragePolicy("default")->getAnyDisk();
            String data_relative_path = "data/" +  escaped_database + "/" + escaped_table + "/";
            LOG_TRACE(log, "Unique Scan local disk path =  {} data_relative_path = {}", local_disk->getPath(), data_relative_path);
            if (local_disk->exists(data_relative_path + it->first))
                part_names.add(it->first);
            else
                throw Exception(
                    "Part: " + it->first + " in snapshot doesn't exist in dir: " + data_relative_path + ", please check and retry. ",
                    ErrorCodes::NO_SUCH_DATA_PART);
        }
    }

    LOG_DEBUG(log, "Found active data parts = {}", part_names.size());

    for (const auto & data_path : data_paths)
    {
        ActiveDataPartSet local_disk_part_names(cloud.format_version);
        for (const auto & part_name : part_names.getParts())
        {
            if (Poco::File(data_path + part_name).exists())
                local_disk_part_names.add(part_name);
        }

        LOG_TRACE(log, "Unique data path = {} local_disk_part_names size = {}", data_path, local_disk_part_names.size());
        active_parts_multi_disk.insert(std::make_pair(data_path, std::move(local_disk_part_names)));
    }
}
void ClickHouseDumper::processTable(const String & database, const String & table,
                                    const String & partition,
                                    const std::vector<String>& partition_list,
                                    const std::vector<String>& skip_partition_list)
{
    String db_table = database + "." + table;
    String escaped_database = escapeForFileName(database);
    String escaped_table = escapeForFileName(table);

    /// Create storage from metadata
    String attach_query_path = config().getString("path") + "/metadata/" + escaped_database + "/" + escaped_table + ".sql";
    LOG_TRACE(log, " attach_query_path is {}", attach_query_path);
    if (!Poco::File(attach_query_path).exists())
        throw Exception("Table " + db_table + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    ReadBufferFromFile in(attach_query_path, 1024);
    String attach_query_str;
    readStringUntilEOF(attach_query_str, in);

    bool is_atomic_database = false;
    auto storage = createStorageFromCreateQuery(attach_query_str, database, table, *global_context, is_atomic_database);
    if (!storage)
    {
        LOG_ERROR(log, "{} is not MergeTree: {}", db_table, attach_query_str);
        return;
    }
    auto & cloud = dynamic_cast<StorageCloudMergeTree &>(*storage);
    Snapshot snapshot;
    SCOPE_EXIT({
        if (cloud.getInMemoryMetadataPtr()->hasUniqueKey())
            unique_table_dump_helper.removeDumpVersionFromZk(*global_context);
    });

    /// Scan parts
    bool is_multi_disk = config().has("multi_disk_path_list");
    std::vector<String> data_paths;
    if (is_multi_disk && is_atomic_database)
        throw Exception("Table " + db_table + " is atomic database : " + attach_query_str, ErrorCodes::UNKNOWN_TABLE);

    initDataDiskPath(escaped_database, escaped_table, data_paths, is_multi_disk);

    /// Get unique table snapshot
    if (cloud.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        for (const auto & data_path : data_paths)
        {
            if (!Poco::File(data_path + "/manifest/").exists())
            {
                LOG_DEBUG(log, "skip not exists manifest {}", data_path);
                continue;
            }
            snapshot = unique_table_dump_helper.getUniqueTableSnapshot(
                escaped_database, escaped_table, data_path + "/manifest/", *global_context);
        }
    }

    auto can_dump = [&](String partition_id) ->bool
    {
        if(!partition.empty() && partition_id != partition)
            return false;

        if(!partition_list.empty() && std::find(std::begin(partition_list),std::end(partition_list),
                                     partition_id) == std::end(partition_list))
            return false;

        if(!skip_partition_list.empty() && std::find(std::begin(skip_partition_list),std::end(skip_partition_list),
                                     partition_id) != std::end(skip_partition_list))
            return false;

        return true;
    };

    /// look through from local disk, get active data part
    std::map<String, ActiveDataPartSet> active_parts_multi_disk;
    if (cloud.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        getUniqueTableActivePartsFromDisk(cloud,
            snapshot, escaped_database, escaped_table, data_paths, active_parts_multi_disk, is_multi_disk, can_dump);
    }
    else
    {
        ActiveDataPartSet part_names(cloud.format_version);
        for (const auto & data_path : data_paths)
        {
            if (!Poco::File(data_path).exists())
            {
                LOG_TRACE(log, "skip not exists disk path = {}", data_path);
                continue;
            }

            LOG_TRACE(log, "Scan local disk path = {}", data_path);
            for (Poco::DirectoryIterator it(data_path); it != Poco::DirectoryIterator(); ++it)
            {
                MergeTreePartInfo part_info;
                if (!MergeTreePartInfo::tryParsePartName(it.name(), &part_info, cloud.format_version))
                    continue;

                if(!can_dump(part_info.partition_id))
                    continue;

                part_names.add(it.name());
            }
        }

        for (const auto & data_path : data_paths)
        {
            ActiveDataPartSet local_disk_part_names(cloud.format_version);
            for (const auto & part_name : part_names.getParts())
            {
                if (Poco::File(data_path + part_name).exists())
                    local_disk_part_names.add(part_name);
            }

            active_parts_multi_disk.insert(std::make_pair(data_path, std::move(local_disk_part_names)));
        }
    }

    DiskPtr remote_disk = global_context->getStoragePolicy("cnch_default_hdfs")->getAnyDisk();
    if (remote_disk->getType() == DiskType::Type::ByteS3)
        throw Exception("Currently dump to  " + DiskType::toString(remote_disk->getType()) + " doesn't supported.", ErrorCodes::UNKNOWN_TABLE);

    for (auto & [data_path, part_names] : active_parts_multi_disk)
    {
        LOG_DEBUG(log, "Found in {} active parts = {}", data_path, part_names.size());

        String tmp_path = "data/" + database + "/" + table + "/";
        String disk_path = data_path.substr(0, data_path.length() - tmp_path.length());
        std::shared_ptr<IDisk> local_disk = std::make_shared<DiskLocal>(data_path, disk_path, DB::DiskStats{});
        startLoadAndDumpPart(cloud, part_names, snapshot, local_disk, remote_disk);

        LOG_DEBUG(log, "Dumped {} parts under disk path {}", part_names.size(), local_disk->getPath());
    }

    /// remote disk remove uuid dir
    String remote_uuid_rel_dir = cloud.getRelativeDataPath(IStorage::StorageLocation::MAIN);
    LOG_DEBUG(log, "start remove remote uuid dir {} remote disk path = {}", remote_uuid_rel_dir, remote_disk->getPath());
    if(remote_disk->exists(remote_uuid_rel_dir))
        remote_disk->removeRecursive(remote_uuid_rel_dir);
}

void ClickHouseDumper::startLoadAndDumpPart(
    StorageCloudMergeTree & cloud,
    const ActiveDataPartSet & part_names,
    Snapshot & snapshot,
    const std::shared_ptr<IDisk> & local_disk,
    const std::shared_ptr<IDisk> & remote_disk)
{
    S3ObjectMetadata::PartGeneratorID part_generator_id(S3ObjectMetadata::PartGeneratorID::DUMPER,
        UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()));
    size_t num_threads = std::min(part_names.size(), size_t(config().getUInt("parallel", 1)));
    MergeTreeCNCHDataDumper dumper(cloud, part_generator_id);

    String to_path = cloud.getDatabaseName() + '/' + cloud.getTableName() + '/';
    if (cloud.getInMemoryMetadataPtr()->hasUniqueKey())
        to_path += DeleteBitmapMeta::delete_files_dir;

    if(!remote_disk->exists(to_path))
        remote_disk->createDirectories(to_path);

    if (num_threads <= 1)
    {
        for (auto & name : part_names.getParts())
        {
            loadAndDumpPart(cloud, dumper, name, snapshot[name], local_disk, remote_disk);
        }
    }
    else
    {
        ThreadPool thread_pool(num_threads);
        auto name_vec = part_names.getParts();
        for (size_t i = 0; i < name_vec.size(); ++i)
        {
            thread_pool.scheduleOrThrowOnError([&, i]
            {
                loadAndDumpPart(cloud, dumper, name_vec[i], snapshot[name_vec[i]], local_disk, remote_disk);
            });
        }
        thread_pool.wait();
    }
}

void ClickHouseDumper::loadAndDumpPart(
    StorageCloudMergeTree & cloud,
    MergeTreeCNCHDataDumper & dumper,
    const String & part_name,
    const UInt64 version,
    const std::shared_ptr<IDisk> & local_disk,
    const std::shared_ptr<IDisk> & remote_disk)
{
    bool overwrite = config().has("overwrite");
    const String to_path = '/' + cloud.getDatabaseName() + '/' + cloud.getTableName() + '/';
    const String full_path = remote_disk->getPath() + to_path + part_name;
    if(!overwrite && remote_disk->exists(full_path))
    {
        LOG_WARNING(log, "Part " + part_name + " already exists. Ignore it.");
        return;
    }

    auto volume = std::make_shared<SingleDiskVolume>("volume_single", local_disk, 0);
    const String relative_path = local_disk->getPath() + "/data/" +  cloud.getDatabaseName() + "/" + cloud.getTableName() + "/" + part_name;
    const String from_path =  cloud.getRelativeDataPath(IStorage::StorageLocation::MAIN) + '/' + part_name;

    auto part_info = MergeTreePartInfo::fromPartName(part_name, cloud.format_version);
    MergeTreeMetaBase::MutableDataPartPtr local_part = std::make_shared<MergeTreeDataPartWide>(
        cloud,
        part_name,
        part_info,
        volume,
        relative_path,
        nullptr,
        IStorage::StorageLocation::AUXILITY);

    bool skip_corrupt_parts = config().has("skip_corrupt_parts");
    try
    {
        LOG_TRACE(log, "Loading {}", part_name);

        local_part->loadColumnsChecksumsIndexes(true, true);
        local_part->low_priority = true;
        if (cloud.getInMemoryMetadataPtr()->hasUniqueKey())
            unique_table_dump_helper.generateUniqueIndexFileIfNeed(local_part, cloud, local_disk);

        LOG_TRACE(log, "Dumping {}", local_part->name);

        auto dumped_part = dumper.dumpTempPart(local_part, remote_disk, true);
        if (cloud.getInMemoryMetadataPtr()->hasUniqueKey())
            unique_table_dump_helper.loadAndDumpDeleteBitmap(cloud, local_part, version, local_disk);

        dumped_part->is_temp = false;
        dumped_part->renameTo(local_part->name, true);

        if (remote_disk->exists(full_path))
        {
            LOG_WARNING(log, "remote disk {} already exists output path {}, start to remove it...", remote_disk->getPath(), full_path);
            remote_disk->removeRecursive(full_path);
        }

        /// remote disk from uuid path move to {database}/{table}
        remote_disk->moveDirectory(from_path, remote_disk->getPath() + to_path);
    }
    catch (const DB::Exception & e)
    {
        if (!skip_corrupt_parts)
            throw;
        LOG_ERROR(log, "Failed to load or dump the part: {}, exception: {}", part_name, e.what());
    }
}

int ClickHouseDumper::main(const std::vector<String> &)
{
    ThreadStatus thread_status;

    /// Load config files if exists
    if (config().has("config-file") || Poco::File("config.xml").exists())
    {
        const auto config_path = config().getString("config-file", "config.xml");
        ConfigProcessor config_processor(config_path, false, true);
        // Mark this configuration as writable and in PRIO_APPLICATION so we can modify it's content when initPath()
        config().add(config_processor.loadConfig().configuration.duplicate(), PRIO_APPLICATION, true, false);
    }

    logger().setLevel(config().getString("logger.level", "debug"));
    log = getLogger(logger());
    unique_table_dump_helper.setLog(log);

    shared_context = DB::Context::createShared();
    global_context = DB::Context::createGlobal(shared_context.get());
    SCOPE_EXIT(global_context->shutdown());
    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);
    if (config().has("macros"))
    {
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));
        /// try to parse current shard index from macros
        const std::map<String, String> macro_map = global_context->getMacros()->getMacroMap();
        if (macro_map.count("shard_num"))
            current_shard_number = parse<Int64>(macro_map.at("shard_num"));
    }

    /// We will terminate process on error
    static KillingErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    registerFunctions();
    registerAggregateFunctions();
    registerDisks();
    registerStorages();

    initPath();
    initHDFS();

    String database = config().getString("database");
    String table = config().getString("table", "");
    String partition = config().getString("partition", "");
    String partition_list = config().getString("partition_list","");
    String skip_partition_list =  config().getString("skip_partition_list","");
    std::vector<String> partition_vec;
    std::vector<String> skip_partition_vec;

    if(!partition_list.empty())
        boost::split(partition_vec, partition_list, boost::is_any_of(","));

    if(!skip_partition_list.empty())
        boost::split(skip_partition_vec, skip_partition_list, boost::is_any_of(","));

    if (table.empty())
        processDatabase(database);
    else
        processTable(database, table, partition, partition_vec, skip_partition_vec);

    return {};
}

}
int mainEntryClickHouseDumper(int argc, char ** argv)
{
    try
    {
        DB::ClickHouseDumper app;
        app.init(argc, argv);
        return app.run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
