#include <iostream>
#include <Common/AdditionalServices.h>
#include <Common/Exception.h>

namespace DB
{

bool AdditionalServices::enabled([[maybe_unused]] AdditionalService::Value svc) const
{
    return enabled_services & 1 << static_cast<int>(svc);
}

void AdditionalServices::throwIfDisabled(AdditionalService::Value svc) const
{
    if (!enabled(svc))
    {
        throw DB::Exception(
            ErrorCodes::FEATURE_NOT_SUPPORT_GIS + svc, "Additional service {} is not supported.", AdditionalService(svc).toString());
    }
}
size_t AdditionalServices::size() const
{
    return std::popcount(enabled_services.load());
}
AdditionalService::AdditionalService(std::string svc_s) noexcept
{
    for (size_t i = 0; i < ValueS.size(); ++i)
    {
        if (svc_s == ValueS[i])
        {
            additional_service = static_cast<Value>(i);
            return;
        }
    }

    LOG_WARNING(getLogger("AdditionalService"), "cannot find additional service {}", svc_s);
    additional_service = Value::SIZE;
}
void AdditionalServices::parseAdditionalServicesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    size_t new_enabled = {};
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("additional_services", keys);
    for (const std::string & service_name : keys)
    {
        if (config.getBool("additional_services." + service_name))
        {
            AdditionalService as = AdditionalService::tryDeserialize(service_name);
            if (as.value() != AdditionalService::SIZE)
            {
                new_enabled |= 1 << static_cast<int>(as);
            }
        }
    }

    repace(new_enabled);
}
void AdditionalServices::repace(size_t new_enabled_services)
{
    enabled_services = new_enabled_services;
}
AdditionalService AdditionalService::tryDeserialize(std::string svc_s) noexcept
{
    return AdditionalService(svc_s);
}
};
