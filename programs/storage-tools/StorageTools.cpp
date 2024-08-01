#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <Poco/DirectoryIterator.h>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <fmt/format.h>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include "PartInspector.h"
// TODO(shiyuze): support check_s3
// #include "S3StorageChecker.h"

using namespace DB;

namespace po = boost::program_options;

int mainEntryStorageTools(int argc, char** argv) {
    try {
        po::options_description desc("storage tools");

        desc.add_options()
            ("log_level", po::value<String>()->default_value("information"), "logging level of storage tools")
            ("command", po::value<String>(), "command to use, supported are inspect_part/help")
            ("args", po::value<std::vector<String>>(), "command arguments");

        po::positional_options_description pos_desc;
        pos_desc.add("command", 1).add("args", -1);

        po::variables_map vm;
        po::parsed_options parsed_opts = po::command_line_parser(argc, argv)
            .options(desc)
            .positional(pos_desc)
            .allow_unregistered()
            .run();

        po::store(parsed_opts, vm);
        po::notify(vm);

        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
        Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
        Poco::Logger::root().setLevel(vm["log_level"].as<String>());
        Poco::Logger::root().setChannel(channel);

        if (!vm.count("command")) {
            std::cerr << "Missing required field command\n" << desc << std::endl;
            return 1;
        }

        String command = vm["command"].as<String>();

        if (command == "help") {
            std:: cout << desc << std::endl;
        } else if (command == "inspect_part") {
            std::vector<std::string> opts = po::collect_unrecognized(parsed_opts.options, po::include_positional);
            opts.erase(opts.begin());
            return parseAndRunInspectPartTask(opts);
        // } else if (command == "check_s3") {
        //     std::vector<std::string> opts = po::collect_unrecognized(parsed_opts.options, po::include_positional);
        //     opts.erase(opts.begin());
        //     return parseAndRunS3CheckTask(opts);
        } else {
            std::cerr << "Unknown command " << command << ", options are:\n"
                << desc << std::endl;
        }
    } catch (...) {
        std::cerr << getCurrentExceptionMessage(true) << std::endl;
        return 1;
    }

    return 0;
}
