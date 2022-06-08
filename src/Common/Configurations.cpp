#include <Common/Configurations.h>

namespace DB
{

void RootConfiguration::loadFromPocoConfigImpl(const PocoAbstractConfig & config, const String &)
{
    resource_manager.loadFromPocoConfig(config, "rm_service");
    resource_manager.loadFromPocoConfig(config, "resource_manager");

    bytejournal.loadFromPocoConfig(config, "bytejournal");

    service_discovery.loadFromPocoConfig(config, "service_discovery");
}

}
