#pragma once

#include "Type.h"
#include <iostream>
#include <memory>
#include "spdlog/spdlog.h"
#include "spdlog/async.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/sinks/basic_file_sink.h"

namespace VolcengineTos {

static LogLevel tosSDKLogLevel = LogLevel::LogOff;
static std::string loggerName = "tos";
static std::string logFilePath = "";
static int64_t logMaxFileSize = 5242880;
static int logMaxFiles = 3;

class LogUtils {
public:
    static LogUtils* instance() {
        static LogUtils instance;
        return &instance;
    }
    static void SetLogger(const std::string& filePath, const std::string& name, LogLevel level) {
        tosSDKLogLevel = level;
        loggerName = name;
        logFilePath = filePath;
        InitInnerLog();
    }
    static void SetLogger(const std::string& filePath, const std::string& name, LogLevel level, int64_t maxFileSize,
                          int maxFiles) {
        tosSDKLogLevel = level;
        loggerName = name;
        logFilePath = filePath;
        logMaxFileSize = maxFileSize;
        logMaxFiles = maxFiles;
        InitInnerLog();
    }
    static std::shared_ptr<spdlog::logger> GetLogger() {
        return instance()->getLogger();
    }
    static void SetLoggerLevel(LogLevel loglevel) {
        instance()->setLogLevel(loglevel);
    }

    static void InitInnerLog() {
        instance();
        SetLoggerLevel(tosSDKLogLevel);
    }
    static void CloseLogger() {
        tosSDKLogLevel = LogLevel::LogOff;
        spdlog::drop(loggerName);  // 释放logger
    }

private:
    std::shared_ptr<spdlog::logger> getLogger() const {
        return logger_;
    }
    void setLogLevel(LogLevel level) {
        // debug< info< warn< error< critical  日志信息低于设置的级别时,不予显示
        if (level == LogDebug) {
            logger_->set_level(spdlog::level::debug);
        }
        if (level == LogInfo) {
            logger_->set_level(spdlog::level::info);
        }
        if (level == LogOff) {
            logger_->set_level(spdlog::level::off);
        }
    }

public:
    LogUtils() {
        if (!logFilePath.empty()) {
            init_logger();
            setLogLevel(tosSDKLogLevel);
        }
    }

    ~LogUtils() = default;

private:
    void init_logger() {
        // _mt 为线程安全的日志
        logger_ = spdlog::rotating_logger_mt(loggerName, logFilePath, logMaxFileSize, logMaxFiles);
    }

private:
    std::shared_ptr<spdlog::logger> logger_ = nullptr;
};
}  // namespace VolcengineTos
