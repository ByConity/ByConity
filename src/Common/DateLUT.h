#pragma once

#include <Common/DateLUTImpl.h>

#include <common/defines.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<const Context>;
}

class DateLUTImpl;


/// This class provides lazy initialization and lookup of singleton DateLUTImpl objects for a given timezone.
class DateLUT : private boost::noncopyable
{
public:
    /// The default instance will return singleton DateLUTImpl for the server time zone.
    /// It may be set using 'timezone' server setting.
    static ALWAYS_INLINE const DateLUTImpl & serverTimezoneInstance()
    {
        const auto & date_lut = getInstance();
        return *date_lut.default_impl.load(std::memory_order_acquire);
    }

    /// Return singleton DateLUTImpl instance for a given time zone.
    static ALWAYS_INLINE const DateLUTImpl & instance(const std::string & time_zone)
    {
        if (time_zone.empty())
            return sessionInstance();

        const auto & date_lut = getInstance();
        return date_lut.getImplementation(time_zone);
    }

    /// Return DateLUTImpl instance for session timezone.
    /// session_timezone is a session-level setting.
    /// If setting is not set, returns the server timezone.
    static const DateLUTImpl & sessionInstance();

    static void setDefaultTimezone(const std::string & time_zone)
    {
        auto & date_lut = getInstance();
        const auto & impl = date_lut.getImplementation(time_zone);
        date_lut.default_impl.store(&impl, std::memory_order_release);
    }

protected:
    DateLUT();

private:
    static DateLUT & getInstance();

    static std::string extractTimezoneFromContext(DB::ContextPtr query_context);

    const DateLUTImpl & getImplementation(const std::string & time_zone) const;

    using DateLUTImplPtr = std::unique_ptr<DateLUTImpl>;

    /// Time zone name -> implementation.
    mutable std::unordered_map<std::string, DateLUTImplPtr> impls;
    mutable std::mutex mutex;

    std::atomic<const DateLUTImpl *> default_impl;
};
