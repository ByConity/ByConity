#include "IMetaClient.h"
#ifdef USE_HIVE

namespace DB
{

void IMetaClient::getConfigValue(std::string & value, const std::string & /*name*/, const std::string & defaultValue)
{
    value = defaultValue;
}

std::shared_ptr<ApacheHive::Database> IMetaClient::getDatabase(const String & /*db_name*/)
{
    return nullptr;
}

}
#endif
