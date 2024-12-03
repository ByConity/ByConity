/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

/// Macros for convenient usage of Poco logger.

#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>
#include <Common/Logger.h>


namespace
{
    template <typename... Ts> constexpr size_t numArgs(Ts &&...) { return sizeof...(Ts); }
    template <typename T, typename... Ts> constexpr auto firstArg(T && x, Ts &&...) { return std::forward<T>(x); }
}


/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as template with {}-substitutions
///  and the latter arguments treat as values to substitute.
/// If only one argument is provided, it is threat as message without substitutions.

/// query logs level has highest priority, if it's specified, it decides whether to log or not.
/// priority: query_logs_level > config_logs_level = client_logs_level
#define LOG_IMPL(logger, priority, PRIORITY, ...) do                              \
{                                                                                 \
    const bool _is_clients_log = (DB::CurrentThread::getGroup() != nullptr) &&    \
        (DB::CurrentThread::getGroup()->client_logs_level >= (priority));         \
    const bool _query_logs_level_specified = (DB::CurrentThread::getGroup() != nullptr) && \
        (DB::CurrentThread::getGroup()->query_logs_level_for_poco != 0);          \
    bool _print_log = false;                                                      \
    const auto & _logger = (logger);                                              \
    if (_query_logs_level_specified)                                              \
    {                                                                             \
        if (DB::CurrentThread::getGroup()->query_logs_level_for_poco >= (PRIORITY))       \
            _print_log = true;                                                    \
    }                                                                             \
    else if ((_logger)->is((PRIORITY)) || _is_clients_log)                        \
        _print_log = true;                                                        \
    if (_print_log)                                                               \
    {                                                                             \
        std::string formatted_message = numArgs(__VA_ARGS__) > 1 ? fmt::format(__VA_ARGS__) : firstArg(__VA_ARGS__); \
        if (auto _channel = (_logger)->getChannel())                              \
        {                                                                         \
            std::string file_function;                                            \
            file_function += __FILE__;                                            \
            file_function += "; ";                                                \
            file_function += __PRETTY_FUNCTION__;                                 \
            Poco::Message poco_message((_logger)->name(), formatted_message,      \
                                 (PRIORITY), file_function.c_str(), __LINE__);    \
            _channel->log(poco_message);                                          \
        }                                                                         \
    }                                                                             \
} while (false)


#define LOG_TRACE(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::trace, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_DEBUG(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_INFO(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, DB::LogsLevel::warning, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_ERROR(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FATAL(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_FATAL, __VA_ARGS__)


/// Compatibility for external projects.
#if defined(ARCADIA_BUILD)
    using Poco::Logger;
    using Poco::Message;
    using DB::LogsLevel;
    using DB::CurrentThread;
#endif
