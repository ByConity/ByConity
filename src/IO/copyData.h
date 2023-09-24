#pragma once

#include <atomic>
#include <functional>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
class Throttler;
class IReservation;

using ThrottlerPtr = std::shared_ptr<Throttler>;

constexpr auto WRITE_RESERVATION_UPDATE_INTERVAL = 64 * 1024 * 1024;

/// Copies data from ReadBuffer to WriteBuffer, all that is.
void copyData(ReadBuffer & from, WriteBuffer & to, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);

/// Copies `bytes` bytes from ReadBuffer to WriteBuffer. If there are no `bytes` bytes, then throws an exception.
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);

/// The same, with the condition to cancel.
void copyData(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);

void copyData(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::function<void()> cancellation_hook, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);

/// Same as above but also use throttler to limit maximum speed
void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);
void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler, IReservation* reservation = nullptr, size_t reservation_update_interval = WRITE_RESERVATION_UPDATE_INTERVAL);

}
