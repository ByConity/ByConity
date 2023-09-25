#include <Common/Exception.h>
#include <Common/Throttler.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/copyData.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

namespace
{

void copyDataImpl(ReadBuffer & from, WriteBuffer & to, bool check_bytes, size_t bytes, const std::atomic<int> * is_cancelled, ThrottlerPtr throttler, IReservation* reservation, size_t reservation_update_interval)
{
    size_t written = 0;

    /// If read to the end of the buffer, eof() either fills the buffer with new data and moves the cursor to the beginning, or returns false.
    while (bytes > 0 && !from.eof())
    {
        if (is_cancelled && *is_cancelled)
            return;

        /// buffer() - a piece of data available for reading; position() - the cursor of the place to which you have already read.
        size_t count = std::min(bytes, static_cast<size_t>(from.buffer().end() - from.position()));
        to.write(from.position(), count);
        from.position() += count;
        bytes -= count;
        written += count;

        if (reservation != nullptr && written >= reservation_update_interval)
        {
            size_t remain_size = reservation->getSize() < written ? 0 : reservation->getSize() - written;
            reservation->update(remain_size);
            written = 0;
        }

        if (throttler)
            throttler->add(count);
    }

    if (check_bytes && bytes > 0)
        throw Exception("Attempt to read after EOF.", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}

void copyDataImpl(ReadBuffer & from, WriteBuffer & to, bool check_bytes, size_t bytes, std::function<void()> cancellation_hook, ThrottlerPtr throttler, IReservation* reservation, size_t reservation_update_interval)
{
    size_t written = 0;

    /// If read to the end of the buffer, eof() either fills the buffer with new data and moves the cursor to the beginning, or returns false.
    while (bytes > 0 && !from.eof())
    {
        if (cancellation_hook)
            cancellation_hook();

        /// buffer() - a piece of data available for reading; position() - the cursor of the place to which you have already read.
        size_t count = std::min(bytes, static_cast<size_t>(from.buffer().end() - from.position()));
        to.write(from.position(), count);
        from.position() += count;
        bytes -= count;
        written += count;

        if (reservation != nullptr && written >= reservation_update_interval)
        {
            size_t remain_size = reservation->getSize() < written ? 0 : reservation->getSize() - written;
            reservation->update(remain_size);
            written = 0;
        }

        if (throttler)
            throttler->add(count);
    }

    if (check_bytes && bytes > 0)
        throw Exception("Attempt to read after EOF.", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}

}

void copyData(ReadBuffer & from, WriteBuffer & to, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), nullptr, nullptr, reservation, reservation_update_interval);
}

void copyData(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), &is_cancelled, nullptr, reservation, reservation_update_interval);
}

void copyData(ReadBuffer & from, WriteBuffer & to, std::function<void()> cancellation_hook, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), cancellation_hook, nullptr, reservation, reservation_update_interval);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, true, bytes, nullptr, nullptr, reservation, reservation_update_interval);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, true, bytes, &is_cancelled, nullptr, reservation, reservation_update_interval);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::function<void()> cancellation_hook, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, true, bytes, cancellation_hook, nullptr, reservation, reservation_update_interval);
}

void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), &is_cancelled, throttler, reservation, reservation_update_interval);
}

void copyDataWithThrottler(ReadBuffer & from, WriteBuffer & to, size_t bytes, const std::atomic<int> & is_cancelled, ThrottlerPtr throttler, IReservation* reservation, size_t reservation_update_interval)
{
    copyDataImpl(from, to, true, bytes, &is_cancelled, throttler, reservation, reservation_update_interval);
}

}
