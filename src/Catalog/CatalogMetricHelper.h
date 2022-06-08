#pragma once

#include <functional>
#include <Common/ProfileEvents.h>


namespace DB
{

namespace Catalog
{
    using Job = std::function<void()>;

    static void runWithMetricSupport(const Job & job, const ProfileEvents::Event & success, const ProfileEvents::Event & failed)
    {
        try
        {
            job();
            ProfileEvents::increment(success);
        }
        catch (...)
        {
            ProfileEvents::increment(failed);
            throw;
        }
    }
}
}
