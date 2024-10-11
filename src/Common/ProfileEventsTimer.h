#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace DB
{

class ProfileEventsTimer
{
public:
    explicit ProfileEventsTimer(ProfileEvents::Event event_)
        : ProfileEventsTimer(event_, event_) {}
    
    ProfileEventsTimer(ProfileEvents::Event request_event_, ProfileEvents::Event time_event_, UInt64 divide_ = 1000)
       : divide(divide_), request_event(request_event_), time_event(time_event_) {}

    ~ProfileEventsTimer()
    {
        /// A bit of a hack, but it's the easiest way to get the same behavior as before.
        /// Only increase request event when manually set.
        if (request_event != time_event)
            ProfileEvents::increment(request_event, 1);
        ProfileEvents::increment(time_event, watch.elapsedNanoseconds() / divide);
    }

private:
    UInt64 divide;
    Stopwatch watch;
    ProfileEvents::Event request_event, time_event;
};

}
