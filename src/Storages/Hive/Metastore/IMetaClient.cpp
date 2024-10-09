#include <memory.h>
#include "Common/config.h"
#ifdef USE_HIVE
#    include "HiveMetastore.h"
#    include "GlueMetastore.h"
#    if USE_JAVA_EXTENSIONS
#        include "JNILfMetastore.h"
#    endif
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void IMetaClient::getConfigValue(std::string & value, const std::string & /*name*/, const std::string & defaultValue)
{
    value = defaultValue;
}

std::shared_ptr<ApacheHive::Database> IMetaClient::getDatabase(const String & /*db_name*/)
{
    return nullptr;
}

std::shared_ptr<IMetaClient> LakeMetaClientFactory::create(const String & name, const std::shared_ptr<CnchHiveSettings> & settings)
{
    if (settings->meta_type.value == "hive")
    {
        return HiveMetastoreClientFactory::instance().getOrCreate(name,settings); 
    }
    if (settings->meta_type.value == "glue")
    {
        return GlueMetastoreClientFactory::instance().getOrCreate(name,settings);
    }
#   if USE_JAVA_EXTENSIONS
    if (settings->meta_type.value == "lf")
    {
        return JNILfMetastoreClientFactory::instance().getOrCreate(name,settings);
    }
#    endif
    
    throw DB::Exception("not implemented", ErrorCodes::BAD_ARGUMENTS);
}
}
#endif
