#pragma once

#include <Common/Logger.h>
#include <sstream>
#include <string>
#include <Poco/FileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/JSON/Template.h>
#include <Poco/PatternFormatter.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/XMLConfiguration.h>
#include <common/logger_useful.h>

#include "Common/Exception.h"
#include "common/types.h"
#include "Core/UUID.h"
#include "Dictionaries/registerDictionaries.h"
#include "Disks/registerDisks.h"
#include "FormaterTool/PartMergerImpl.h"
#include "Formats/registerFormats.h"
#include "Functions/registerFunctions.h"
#include "Interpreters/Context.h"
#include "Storages/registerStorages.h"
#include "loggers/OwnFormattingChannel.h"

using Poco::Util::Application;
using Poco::Util::OptionSet;

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}


/**
 * @class PartMergerApp
 * @brief Serves as a main application interface for part-merger.
 * It is responsible for dealing with basic CLI arguments
 * processing and context-related resources initialize.
 */
class PartMergerApp : public Application

{
public:
    const static std::string help_message;
    const static std::string default_config;
    /**
     * Print help message into console.
     */
    PartMergerApp() = default;


private:
    /**
     * Print help message for part_merger_tool.
     */
    inline static void mergerHelp(LoggerPtr log) { LOG_ERROR(log, PartMergerApp::help_message); }

    /**
     * Init HDFS default configuration.
     */
    void initHDFS(DB::ContextMutablePtr context, LoggerPtr log);

    int main([[maybe_unused]] const std::vector<DB::String> & args) override;

    void defineOptions(OptionSet & options) override;
};
