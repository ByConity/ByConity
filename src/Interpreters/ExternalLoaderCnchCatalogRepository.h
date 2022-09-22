#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

namespace Catalog
{
class Catalog;
}

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
    static std::optional<UUID> resolveDictionaryName(const std::string & name, ContextPtr context);
private:
    ContextPtr context;
    std::shared_ptr<Catalog::Catalog> catalog;
};

}
