#include "PartMergerApp.h"
#include <Server/ServerHelper.h>

/**
 * @brief Use `./part-merger/` instead of `./` to reduce disturbance to the root directory.
 */

const String part_merger_path = "part-merger/";
const std::string PartMergerApp::help_message
    = "Usage:\n\t"
      "part_merger_tool\n\t\t"
      " --config-file /path/to/optional/config.xml\n\t\t"
      " --uuids uuid_list (separated with \",\")\n\t\t"
      " --create-table-sql \"CREATE TABLE db.test (...) ENGINE = CloudMergeTree(db, test) PARTITION BY x ORDER BY y...\"\n\t\t"
      " --source-path hdfs://path/to/source/parts/dir/\n\t\t"
      " --output-path hdfs://path/to/output/dir/\n\t\t"
      " --settings setting_a=value_a,setting_b=value_b...";

const std::string PartMergerApp::default_config = "<yandex>\n"
                                                  "<storage_configuration>\n"
                                                  "<disks>\n"
                                                  "    <hdfs>\n"
                                                  "        <path><?= source-path ?></path>\n"
                                                  "        <type>hdfs</type>\n"
                                                  "    </hdfs>\n"
                                                  "    <local>\n"
                                                  "        <path><?= local-path ?></path>\n"
                                                  "    </local>\n"
                                                  "</disks>\n"
                                                  "<policies>\n"
                                                  "    <default>\n"
                                                  "        <volumes>\n"
                                                  "            <hdfs>\n"
                                                  "                <default>hdfs</default>\n"
                                                  "                <disk>hdfs</disk>\n"
                                                  "            </hdfs>\n"
                                                  "            <local>\n"
                                                  "                <default>local</default>\n"
                                                  "                <disk>local</disk>\n"
                                                  "            </local>\n"
                                                  "        </volumes>\n"
                                                  "    </default>\n"
                                                  "</policies>\n"
                                                  "</storage_configuration>\n"
                                                  "<merge_selector>merger</merge_selector>\n"
                                                  "</yandex>";

void PartMergerApp::initHDFS(DB::ContextMutablePtr context, LoggerPtr log)
{
    LOG_DEBUG(log, "Initialize HDFS driver.");
    using HDFSConnectionParams = DB::HDFSConnectionParams;

    /// Init HDFS3 client config path.
    DB::String hdfs_config = config().getString("hdfs3_config", "");
    if (!hdfs_config.empty())
    {
        setenv("LIBHDFS3_CONF", hdfs_config.c_str(), 1);
    }

    /// Options load from command line argument use priority -100 in layeredconfiguration, so construct
    /// hdfs params from config directly rather than from config file.
    HDFSConnectionParams hdfs_params = HDFSConnectionParams(
        HDFSConnectionParams::CONN_NNPROXY, config().getString("hdfs_user", "clickhouse"), config().getString("hdfs_nnproxy", "nnproxy"));
    /// Yield params to the context so that we could get this later.
    context->setHdfsConnectionParams(hdfs_params);

    const int hdfs_max_fd_num = config().getInt("hdfs_max_fd_num", 100000);
    const int hdfs_skip_fd_num = config().getInt("hdfs_skip_fd_num", 100);
    const int hdfs_io_error_num_to_reconnect = config().getInt("hdfs_io_error_num_to_reconnect", 10);
    registerDefaultHdfsFileSystem(hdfs_params, hdfs_max_fd_num, hdfs_skip_fd_num, hdfs_io_error_num_to_reconnect);
}

int PartMergerApp::main([[maybe_unused]] const std::vector<DB::String> & args)
{
    /// Initialize logger.
    ///
    /// Design choice:
    /// We choose to initialize logger after Poco::Application::initialize(),
    /// so the global logger configuration won't get messed up by Poco::Application.
    Poco::AutoPtr<Poco::PatternFormatter> pf = new Poco::PatternFormatter("[%Y-%m-%d %H:%M:%S.%i] <%p> %t");
    pf->setProperty("times", "local");

    Poco::AutoPtr<Poco::SplitterChannel> split_channel = new Poco::SplitterChannel;

    Poco::AutoPtr<Poco::ConsoleChannel> cout_channel = new Poco::ConsoleChannel(std::cout);
    Poco::AutoPtr<Poco::FormattingChannel> fcout_channel = new Poco::FormattingChannel(pf, cout_channel);

    split_channel->addChannel(fcout_channel);

    Poco::AutoPtr<Poco::ConsoleChannel> cerr_channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::FormattingChannel> fcerr_channel = new Poco::FormattingChannel(pf, cerr_channel);
    Poco::AutoPtr<DB::OwnFormattingChannel> of_channel = new DB::OwnFormattingChannel();
    of_channel->setChannel(fcerr_channel);
    of_channel->setLevel(Poco::Message::PRIO_ERROR);
    split_channel->addChannel(of_channel);

    Poco::AutoPtr<Poco::FileChannel> f_channel = new Poco::FileChannel;
    f_channel->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path::current() + "task.log");
    f_channel->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, "true");
    Poco::AutoPtr<Poco::FormattingChannel> ff_channel = new Poco::FormattingChannel(pf, f_channel);
    split_channel->addChannel(ff_channel);

    Poco::Logger::root().setChannel(split_channel);
    if (config().hasOption("verbose"))
    {
        Poco::Logger::root().setLevel("debug");
    }
    else
    {
        Poco::Logger::root().setLevel("information");
    }
    auto log = getLogger("PartMergerApp");

    LOG_DEBUG(log, "Parse arguments");
    // Parse arguments.
    if (config().has("config-file"))
    {
        const auto config_path = config().getString("config-file", "config.xml");
        DB::ConfigProcessor config_processor(config_path, false, false);
        config().add(config_processor.loadConfig().configuration.duplicate(), PRIO_DEFAULT, false);
    }
    else
    {
        /// If no config-file
        if (!config().has("uuids"))
        {
            LOG_ERROR(log, "Argument --uuids is required when --config-file not given.");
            mergerHelp(log);
            return -1;
        }

        if (!config().has("create-table-sql"))
        {
            LOG_ERROR(log, "Argument --create-table-sql is required");
            mergerHelp(log);
            return -1;
        }

        if (!config().has("source-path"))
        {
            LOG_ERROR(log, "Argument --source_path is required");
            mergerHelp(log);
            return -1;
        }

        if (!config().has("output-path"))
        {
            LOG_ERROR(log, "Argument --output_path is required");
            mergerHelp(log);
            return -1;
        }

        // render default config
        Poco::JSON::Object::Ptr params = new Poco::JSON::Object();
        params->set("source-path", config().getString("source-path"));
        params->set("local-path", Poco::Path::current() + "/" + part_merger_path);
        Poco::JSON::Template tpl;
        tpl.parse(default_config);
        std::stringstream out;
        tpl.render(params, out);

        std::string default_xml_config = "<?xml version=\"1.0\"?>";
        default_xml_config = default_xml_config + out.str();

        LOG_DEBUG(log, "config: {}", default_xml_config);

        DB::ConfigProcessor config_processor("", false, false);
        config().add(config_processor.loadConfig(default_xml_config).configuration.duplicate(), PRIO_DEFAULT, false);
    }


    LOG_DEBUG(log, "Initialize context.");
    DB::ThreadStatus status;

    DB::registerFunctions();
    DB::registerDictionaries();
    DB::registerDisks();
    DB::registerStorages();
    DB::registerFormats();
    // Initialize context.
    auto shared_context = DB::Context::createShared();
    auto global_context = DB::Context::createGlobal(shared_context.get());

    // Initialize storage directory.
    //
    std::string path = Coordination::getCanonicalPath(config().getString("path", part_merger_path));
    global_context->setPath(path);
    {
        fs::create_directories(fs::path(path) / "disks/");
    }

    /// Setup storage with temporary data for processing of heavy queries.
    {
        std::string tmp_path = config().getString("tmp_path", path + "tmp/");
        std::string tmp_policy = config().getString("tmp_policy", "");
        const auto & volume = global_context->setTemporaryStorage(tmp_path, tmp_policy);

        for (const auto & disk : volume->getDisks())
            Coordination::setupTmpPath(log, disk->getPath());
        global_context->setTemporaryStoragePath();
    }


    global_context->setCurrentQueryId(DB::UUIDHelpers::UUIDToString(DB::UUIDHelpers::generateV4()));

    /// Apply the config (processed by ConfigProcessor) to global_context.
    {
        Poco::Util::LayeredConfiguration * tmp_config = &config();
        DB::ConfigurationPtr configuration = Poco::AutoPtr(tmp_config);
        global_context->setConfig(configuration);
    }
    global_context->makeGlobalContext();
    global_context->setMarkCache(1000000);


    // Init HDFS.
    initHDFS(global_context, log);

    try
    {
        DB::PartMergerImpl impl{global_context, config(), log};
        impl.execute();
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Interupted by Poco::exception: {}", e.what());
        return -1;
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Interupted by std::exception: {}", e.what());
        return -1;
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknown exception occurs.");
        return -1;
    }

    return 0;
}

void PartMergerApp::defineOptions(OptionSet & options)
{
    Application::defineOptions(options);

    options.addOption(Poco::Util::Option("config-file", "C", "load configuration from a given file (deprecated)")
                          .required(false)
                          .argument("<file>")
                          .binding("config-file"));

    options.addOption(Poco::Util::Option("create-table-sql", "Q", "CREATE SQL for the table")
                          .required(false)
                          .argument("<sql>")
                          .binding("create-table-sql"));

    options.addOption(Poco::Util::Option("uuids", "D", "Subdirs(seperate by comma) of input path which contains source data parts.")
                          .required(false)
                          .argument("<uuids>")
                          .binding("uuids"));

    options.addOption(Poco::Util::Option("source-path", "S", "the source directory of parts to merge")
                          .required(false)
                          .argument("<source>")
                          .binding("source-path"));

    options.addOption(Poco::Util::Option("output-path", "O", "the output dir").required(false).argument("<output>").binding("output-path"));

    options.addOption(Poco::Util::Option("settings", "S", "set settings (name=value,name2=value2...)")
                          .required(false)
                          .argument("<settings>")
                          .binding("settings"));

    options.addOption(Poco::Util::Option("verbose", "V", "print DEBUG level log").required(false).binding("verbose"));

    options.addOption(
        Poco::Util::Option("concurrency", "T", "multi-threading").required(false).argument("<concurrency>").binding("concurrency"));
}

/**
 * Main entry for the application.
 */
int mainEntryClickhousePartMerger(int argc, char ** argv)
{
    Poco::AutoPtr<PartMergerApp> part_merger_app = new PartMergerApp;
    part_merger_app->init(argc, argv);
    return part_merger_app->run();
}
