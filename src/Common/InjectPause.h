#pragma once

#include <folly/Function.h>
#include <folly/Range.h>
#include <common/scope_guard.h>

namespace DB
{
void injectPause(folly::StringPiece name);

/**
 * Allow stopping the process at this named point.
 */
#define INJECT_PAUSE(name) DB::injectPause(#name)

#define ENABLE_INJECT_PAUSE_IN_SCOPE() \
  injectPauseEnabled() = true;         \
  SCOPE_EXIT( { EXPECT_EQ(injectPauseClear(), 0); } )

// Callback that can be executed optionally for INJECT_PAUSE point
using PauseCallback = folly::Function<void()>;

// Default timeout for INJECT_PAUSE wait
constexpr uint32_t kInjectPauseMaxWaitTimeoutMs = 60000;

/**
 * Toggle INJECT_PAUSE() logic on and off
 */
bool & injectPauseEnabled();

/**
 * Stop any thread at this named INJECT_PAUSE point from now on.
 */
void injectPauseSet(folly::StringPiece name, size_t numThreads = 0);

/**
 * The thread will execute the callback at INJECT_PAUSE point and go.
 */
void injectPauseSet(folly::StringPiece name, PauseCallback && callback);

/**
 * If the named INJECT_PAUSE point was set, blocks until the requested
 * number of threads is stopped at it including those already stopped
 * and returns true.
 */
bool injectPauseWait(folly::StringPiece name, size_t numThreads = 1, bool wakeup = true, uint32_t timeoutMs = 0);

/**
 * Stop blocking threads at this INJECT_PAUSE point and unblock any
 * currently waiting threads. If name is not given, all of
 * INJECT_PAUSE points are cleared.
 * Returns the number of INJECT_PAUSE points where threads were stopped
 */
size_t injectPauseClear(folly::StringPiece name = "");
}
