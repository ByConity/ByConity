#pragma once

#include <atomic>
#include <boost/core/noncopyable.hpp>
#include <common/types.h>

#include "safeExit.h"

namespace DB
{

std::atomic<Int32> exit_after_signals = 0;

class QueryInterruptHandler : private boost::noncopyable
{
public:
    /// Store how much interrupt signals can be before stopping the query
    /// by default stop after the first interrupt signal.
    static void start(Int32 signals_before_stop = 1) { exit_after_signals.store(signals_before_stop); }

    /// Set value not greater then 0 to mark the query as stopped.
    static void stop() { return exit_after_signals.store(0); }

    /// Return true if the query was stopped.
    /// Query was stopped if it received at least "signals_before_stop" interrupt signals.
    static bool tryStop() { return exit_after_signals.fetch_sub(1) <= 0; }
    static bool cancelled() { return exit_after_signals.load() <= 0; }

    /// Return how much interrupt signals remain before stop.
    static Int32 cancelledStatus() { return exit_after_signals.load(); }
};

/// This signal handler is set for SIGINT and SIGQUIT.
void interruptSignalHandler(int signum)
{
    if (QueryInterruptHandler::tryStop())
        safeExit(128 + signum);
}
}
