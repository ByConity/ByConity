#pragma once

#include <map>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/MapConfiguration.h>
#include "IExternalCatalog.h"
#include "Transaction/TxnTimestamp.h"


namespace DB::ExternalCatalog
{


class ExternalCatalogConfiguration : public Poco::Util::MapConfiguration {
    public:
        void forEachKey(const std::function<void(const String&, const String&)> & action)
        {
            for(auto it = begin(); it !=end(); ++it){
                action(it->first, it->second);
            }
        }
};


using PlainConfigs = ExternalCatalogConfiguration;
using PlainConfigsPtr = Poco::AutoPtr<PlainConfigs>;

class IExternalCatalogMgr
{
public:
    virtual ~IExternalCatalogMgr() { }
    virtual bool createCatalog(const std::string & catalog_name, PlainConfigs * properties, const TxnTimestamp & ts) = 0;
    virtual ExternalCatalogPtr getCatalog(const std::string & catalog_name) = 0;
    virtual ExternalCatalogPtr tryGetCatalog(const std::string & catalog_name) = 0;
    virtual bool isCatalogExist(const std::string& catalog_name) = 0;
    virtual bool dropExternalCatalog(const std::string & catalog_name) = 0;
    virtual bool alterExternalCatalog(const std::string &, PlainConfigs * new_config) = 0;
};

using ExternalCatalogMgrPtr = std::unique_ptr<IExternalCatalogMgr>;


namespace Mgr
{
    void init(Context & context, const Poco::Util::AbstractConfiguration & conf);
    IExternalCatalogMgr & instance();
    std::string configPrefix(); 
}


}
