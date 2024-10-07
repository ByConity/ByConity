/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <string>
#include <ServiceDiscovery/registerServiceDiscovery.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <FormaterTool/PartConverter.h>
#include <FormaterTool/PartWriter.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Parsers/ASTPartToolKit.h>
#include <Parsers/ParserPartToolkitQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/registerStorages.h>
#include <loggers/OwnFormattingChannel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Environment.h>
#include <Poco/FileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config.h>
#include <common/logger_useful.h>

#include "PartMergerApp.h"

int mainHelp(int, char **)
{
    /// TODO: make help more clear
    std::cout << "Usage : \n";
    std::cout << "clickhouse [part-writer|part-converter] query" << std::endl;
    std::cout << PartMergerApp::help_message << std::endl;

    return 0;
}

void run(const std::string & query, LoggerPtr log)
{
    LOG_DEBUG(log, "Executing query : {}", query);
    DB::ThreadStatus status;

    DB::registerFunctions();
    DB::registerDictionaries();
    DB::registerServiceDiscovery();
    DB::registerDisks();
    DB::registerStorages();
    DB::registerFormats();

    DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration());

    auto shared_context = DB::Context::createShared();
    auto mutable_context_ptr = DB::Context::createGlobal(shared_context.get());
    mutable_context_ptr->makeGlobalContext();
    mutable_context_ptr->setConfig(configuration);
    mutable_context_ptr->setMarkCache(1000000);
    mutable_context_ptr->initServiceDiscoveryClient();

    const char * begin = query.data();
    const char * end = query.data() + query.size();

    DB::ParserPartToolkitQuery parser(end);
    auto ast = DB::parseQuery(parser, begin, end, "", 0, 0);
    const DB::ASTPartToolKit & query_ast = ast->as<DB::ASTPartToolKit &>();

    std::shared_ptr<DB::PartToolkitBase> executor = nullptr;

    if (query_ast.type == DB::PartToolType::WRITER)
        executor = std::make_shared<DB::PartWriter>(ast, mutable_context_ptr);
    else if (query_ast.type == DB::PartToolType::MERGER)
    {
        LOG_ERROR(log, "Part merger is not implemented in cnch. ");
        return;
    }
    else if (query_ast.type == DB::PartToolType::CONVERTER)
        executor = std::make_shared<DB::PartConverter>(ast, mutable_context_ptr);

    executor->execute();
}

int mainEntryClickhousePartToolkit(int argc, char ** argv)
{
    /// init poco log config.
    Poco::AutoPtr<Poco::PatternFormatter> pf = new Poco::PatternFormatter("[%Y-%m-%d %H:%M:%S.%i] <%p> %s: %t");
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

    String log_level = "debug";

    try
    {
        log_level = Poco::Environment::get("LOG_LEVEL", "debug");
    }
    catch (...)
    {
    }

    LoggerPtr log = getLogger("part-toolkit");

    LOG_INFO(log, "Logger level: {}", log_level);

    Poco::Logger::root().setLevel(log_level);

    if (argc < 2)
    {
        mainHelp(argc, argv);
        return 1;
    }

    try
    {
        std::string sql = argv[1];
        run(sql, log);
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        return -1;
    }

    return 0;
}
