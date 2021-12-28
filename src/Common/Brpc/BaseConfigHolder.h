#pragma once

#include <functional>
#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
using RawConfig = Poco::Util::AbstractConfiguration;
using RawConfAutoPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
struct RawConfigDeleter
{
    void operator()(RawConfig * ptr)
    {
        if (ptr)
            ptr->release();
    }
};

class BaseConfigHolder : private boost::noncopyable
{
public:

    explicit BaseConfigHolder() = default;
    virtual ~BaseConfigHolder() = default;
    virtual void init(RawConfAutoPtr conf_ptr) = 0;
    virtual void reload(RawConfAutoPtr conf_ptr) = 0;
};
}
