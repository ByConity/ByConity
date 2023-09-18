#pragma once
#include <memory>
#include <string>
#include <vector>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/Hive/Metastore/IMetaClient.h>
#include <Storages/IStorage_fwd.h>
#include <hivemetastore/hive_metastore_types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/DefaultCatalogName.h>

namespace DB::ExternalCatalog
{


using ThriftPartition = Apache::Hadoop::Hive::Partition;

class IExternalCatalog
{
public:
    virtual ~IExternalCatalog() { }
    // get all db names
    virtual std::string name() = 0;
    virtual std::vector<std::string> listDbNames() = 0;

    // get all table names
    virtual std::vector<std::string> listTableNames(const std::string & db_name) = 0;

    // get partition keys
    virtual std::vector<std::string> listPartitionNames(const std::string & db_name, const std::string & table_name) = 0;

    virtual StoragePtr getTable(const std::string & db_name, const std::string & table_name, ContextPtr local_context) = 0;
    virtual bool isTableExist(const std::string & db_name, const std::string & table_name, ContextPtr local_context) = 0;
    virtual UUID getTableUUID(const std::string & db_name, const std::string & table_name) = 0;

    virtual std::vector<ApacheHive::Partition>
    getPartionsByFilter(const std::string & db_name, const std::string & table_name, const std::string & filter) = 0;
};

using ExternalCatalogPtr = std::shared_ptr<IExternalCatalog>;


}
