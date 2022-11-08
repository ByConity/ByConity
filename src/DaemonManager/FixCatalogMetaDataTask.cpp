#include <DaemonManager/FixCatalogMetaDataTask.h>
#include <Catalog/CatalogFactory.h>
#include <Parsers/ASTCreateQuery.h>
#include <Catalog/Catalog.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DaemonManager
{

UUID getUUIDFromCreateQuery(const DB::Protos::DataModelDictionary & d)
{
    ASTPtr ast = Catalog::CatalogFactory::getCreateDictionaryByDataModel(d);
    ASTCreateQuery * create_ast = ast->as<ASTCreateQuery>();
    UUID uuid_in_create_query = create_ast->uuid;
    return uuid_in_create_query;
}

/// for testing
void createMissingUUIDDictionaryModel(ContextPtr context)
{
    String create_query = "CREATE DICTIONARY test.dict_flat_no_ip_port_bug UUID '4c3f0f76-e616-4195-9c7a-fdf8ce23c8b9' (`id` UInt64, `a` UInt64 DEFAULT 0, `b` Int32 DEFAULT -1, `c` String DEFAULT 'none') PRIMARY KEY id SOURCE(CLICKHOUSE(USER 'default' TABLE 'table_for_no_ip_port_dict' PASSWORD '' DB 'test')) LIFETIME(MIN 1000 MAX 2000) LAYOUT(FLAT())";
    String database = "test";
    String table = "dict_flat_no_ip_port_bug";
    std::shared_ptr<Catalog::Catalog> catalog = context->getCnchCatalog();
    catalog->createDictionary(StorageID{database, table}, create_query);
}


void fixDictionary(Catalog::Catalog * catalog, Poco::Logger * log)
{
    Catalog::Catalog::DataModelDictionaries all = catalog->getAllDictionaries();
    std::for_each(all.begin(), all.end(),
        [& catalog, log] (const DB::Protos::DataModelDictionary & model)
        {
            UUID uuid_field = RPCHelpers::createUUID(model.uuid());
            UUID uuid_in_create_query =  getUUIDFromCreateQuery(model);
            if ((uuid_field == UUIDHelpers::Nil) ||
                (uuid_in_create_query == UUIDHelpers::Nil))
            {
                LOG_INFO(log, "fix Catalog metadata for dictionary {}.{} because missing uuid", model.database(), model.name());
                catalog->fixDictionary(model.database(), model.name());
            }
        });
}

void fixCatalogMetaData(ContextPtr context, Poco::Logger * log)
{
    LOG_INFO(log, "execute fixing Catalog Metadata task");
    std::shared_ptr<Catalog::Catalog> catalog = context->getCnchCatalog();
    fixDictionary(catalog.get(), log);
}

} /// end namespace DaemonManager

} /// end namespace DB
