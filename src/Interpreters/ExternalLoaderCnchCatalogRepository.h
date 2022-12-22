#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Protos/data_models.pb.h>
#include <unordered_map>

namespace DB
{

namespace Catalog
{
class Catalog;
}

class CnchCatalogDictionaryCache
{
public:
    CnchCatalogDictionaryCache(ContextPtr context);
    void loadFromCatalog();
    std::set<std::string> getAllUUIDString() const;
    bool exists(const String & uuid_str) const;
    Poco::Timestamp getUpdateTime(const String & uuid_str) const;
    LoadablesConfigurationPtr load(const String & uuid_str) const;
    std::optional<UUID> findUUID(const StorageID & storage_id) const;
private:
    ContextPtr context;
    std::shared_ptr<Catalog::Catalog> catalog;
    std::unordered_map<String, DB::Protos::DataModelDictionary> data;
    mutable std::mutex data_mutex;
};

/// Cnch Catalog repository used by ExternalLoader
class ExternalLoaderCnchCatalogRepository : public IExternalLoaderConfigRepository
{
public:
    explicit ExternalLoaderCnchCatalogRepository(ContextPtr context);

    std::string getName() const override;

    std::set<std::string> getAllLoadablesDefinitionNames() override;

    bool exists(const std::string & loadable_definition_name) override;

    Poco::Timestamp getUpdateTime(const std::string & loadable_definition_name) override;

    LoadablesConfigurationPtr load(const std::string & loadable_definition_name) override;

    static StorageID parseStorageID(const std::string & loadable_definition_name);
    static std::optional<UUID> resolveDictionaryName(const std::string & name, const std::string & current_database_name, ContextPtr context);
private:
    /// cache data from catalog
    CnchCatalogDictionaryCache & cache;
    std::shared_ptr<Catalog::Catalog> catalog;
};

}
