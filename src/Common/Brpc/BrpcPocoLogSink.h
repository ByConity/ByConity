#pragma once

#include <boost/noncopyable.hpp>
#include <butil/logging.h>
#include <butil/strings/string_piece.h>
#include <common/logger_useful.h>

namespace DB
{
/// A BRPC LogSink for adapting to POCO logging system
class BrpcPocoLogSink : public ::logging::LogSink, private boost::noncopyable
{
public:
    explicit BrpcPocoLogSink() { logger = &Poco::Logger::get("brpc"); }

    ~BrpcPocoLogSink() override = default;

    bool OnLogMessage(int severity, const char * file, int line, const butil::StringPiece & content) override;


private:
    Poco::Logger * logger;
};

static inline Poco::Message::Priority brpc2PocoLogPriority(int brpcLogPriority)
{
    switch (brpcLogPriority)
    {
        case ::logging::BLOG_INFO:
            return Poco::Message::PRIO_INFORMATION;
        case ::logging::BLOG_NOTICE:
            return Poco::Message::PRIO_NOTICE;
        case ::logging::BLOG_WARNING:
            return Poco::Message::PRIO_WARNING;
        case ::logging::BLOG_ERROR:
            return Poco::Message::PRIO_ERROR;
        case ::logging::BLOG_FATAL:
            return Poco::Message::PRIO_FATAL;
        default:
            // mapping brpc multiple verbose_levels to PRIO_TRACE
            return Poco::Message::PRIO_TRACE;
    }
}

static inline int poco2BrpcLogPriority(int pocoLogLevel)
{
    auto poco_log_priority = static_cast<Poco::Message::Priority>(pocoLogLevel);
    switch (poco_log_priority)
    {
        case Poco::Message::PRIO_INFORMATION:
            return ::logging::BLOG_INFO;
        case Poco::Message::PRIO_NOTICE:
            return ::logging::BLOG_NOTICE;
        case Poco::Message::PRIO_WARNING:
            return ::logging::BLOG_WARNING;
        case Poco::Message::PRIO_ERROR:
            return ::logging::BLOG_ERROR;
        case Poco::Message::PRIO_CRITICAL:
        case Poco::Message::PRIO_FATAL:
            return ::logging::BLOG_FATAL;
        case Poco::Message::PRIO_DEBUG:
#ifndef NDEBUG
            return ::logging::BLOG_INFO;
#else
            return ::logging::BLOG_VERBOSE;
#endif
        default:
            return ::logging::BLOG_VERBOSE;
    }
}
}
