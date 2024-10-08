#pragma once

#include <memory>
#include <Poco/Logger.h>

namespace DB
{
/// Lightweight logger that acts mostly like Poco::Logger but is cheaper to create & destroy.
class VirtualLogger
{
public:
    explicit VirtualLogger(const std::string & name_, ::Poco::Logger * wrapped_) : fake_name(name_), wrapped(wrapped_) { }

    /// implement functions used by LOG_IMPL(...) so that it can be used in LOG_XXX macros
    const std::string & name() const { return fake_name; }
    bool is(int level) const { return wrapped->is(level); }
    ::Poco::Channel * getChannel() const { return wrapped->getChannel(); }

    bool fatal() const { return wrapped->fatal(); }
    bool critical() const { return wrapped->critical(); }
    bool error() const { return wrapped->error(); }
    bool warning() const { return wrapped->warning(); }
    bool notice() const { return wrapped->notice(); }
    bool information() const { return wrapped->information(); }
    bool debug() const { return wrapped->debug(); }
    bool trace() const { return wrapped->trace(); }

    int getLevel() const { return wrapped->getLevel(); }
    void log(const ::Poco::Message & msg) { wrapped->log(msg); }

private:
    std::string fake_name;
    ::Poco::Logger * wrapped;
};

} // namespace DB

using LoggerPtr = std::shared_ptr<DB::VirtualLogger>;
using LoggerRawPtr = Poco::Logger *;

/// Factory method to obtain a lightweght logger.
///
/// Advantages over `&Poco::Logger::get(name)` or `getRawLogger`
/// 1. no lock contention during logger creation / destruction
/// 2. no risk to memleak (logger won't be added to global map structure)
LoggerPtr getLogger(const std::string & name);

/// Adaptor interface that convert an existing Poco::Logger into LoggerPtr.
/// It's caller's responsibility to make sure `raw_logger` lives longer,
/// Use with caution.
LoggerPtr getLogger(Poco::Logger & raw_logger);

/** Create raw Poco::Logger that will not be destroyed before program termination.
  * This can be used in cases when specific Logger instance can be singletone.
  *
  * For example you need to pass Logger into low-level libraries as raw pointer, and using
  * RAII wrapper is inconvenient.
  *
  * Generally you should always use getLogger functions.
  */
LoggerRawPtr getRawLogger(const std::string & name);
