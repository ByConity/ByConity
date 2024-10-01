#include <memory>
#include <mutex>
#include <ExternalCatalog/CnchExternalCatalogMgr.h>
#include <ExternalCatalog/HiveExternalCatalog.h>
#include <ExternalCatalog/IExternalCatalogMgr.h>
#include <ExternalCatalog/MockExternalCatalog.h>
#include <IO/WriteBufferFromString.h>
#include <Protos/data_models.pb.h>
#include <boost/algorithm/string.hpp>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include "Catalog/StringHelper.h"
#include "IO/Operators.h"
#include "IO/WriteBufferFromString.h"
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_CATALOG;
}

namespace DB::ExternalCatalog
{
CnchExternalCatalogMgr::CnchExternalCatalogMgr(Context & _context, const Poco::Util::AbstractConfiguration & mgr_conf)
    : context(_context), metastore_conf(mgr_conf, Mgr::configPrefix())
{
    // TODO(ExterncalCatalog):: check whether to set FLAGS_consul_agent_addr here.
    meta_proxy = std::make_shared<Catalog::MetastoreProxy>(metastore_conf, mgr_conf.getBool("enable_cnch_write_remote_catalog", true));
    name_space = mgr_conf.getString(Mgr::configPrefix() + ".name_space", "default");
}
bool CnchExternalCatalogMgr::createCatalog(const std::string & catalog_name, PlainConfigs * catalog_meta, const TxnTimestamp & ts)
{
    static std::string key_type = "type";
    if (!catalog_meta->has(key_type))
    {
        throw Exception("The catalog type of " + catalog_name + " must be assigned.", ErrorCodes::BAD_ARGUMENTS);
    }
    Protos::DataModelCatalog catalog_model;
    catalog_model.set_name(catalog_name);
    catalog_model.set_catalog_type(catalog_meta->getString(key_type));
    auto * proto_properties = catalog_model.mutable_properties();

    catalog_meta->forEachKey([&proto_properties](const String & key, const String & val) {
        auto * entry = proto_properties->Add();
        entry->set_key(key);
        entry->set_value(val);
    });

    catalog_model.set_commit_time(ts.toUInt64());

    meta_proxy->addExternalCatalog(name_space, catalog_model);
    LOG_DEBUG(log, "created catalog {} with {}", catalog_name, catalog_model.DebugString());
    return true;
}

static void protoToConfig(const Protos::DataModelCatalog & catalog_model, PlainConfigs & conf)
{
    for (const auto & property : catalog_model.properties())
    {
        conf.setString(property.key(), property.value());
    }
}

ExternalCatalogPtr CnchExternalCatalogMgr::getCatalog(const std::string & catalog_name)
{
    auto catalog = tryGetCatalog(catalog_name);
    if (!catalog)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot get external catalog {}", catalog_name);
    }
    return catalog;
}


ExternalCatalogPtr CnchExternalCatalogMgr::tryGetCatalog(const std::string & catalog_name)
{
    std::vector<std::string> catalog_info;
    meta_proxy->getExternalCatalog(name_space, catalog_name, catalog_info);
    if (catalog_info.empty())
    {
        LOG_WARNING(log, "The catalog " + catalog_name + " dose not exsit.");
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot get external catalog {}", catalog_name);
    }
    assert(catalog_info.size() == 1);
    const std::string & bin = catalog_info[0];
    Protos::DataModelCatalog catalog_model;
    catalog_model.ParseFromString(bin);
    LOG_TRACE(log, "get catalog {} with {}", catalog_name, catalog_model.DebugString());
    auto type = catalog_model.catalog_type();

    ExternalCatalogPtr ret;
    if (boost::iequals(type, "mock"))
    {
        ret = std::make_shared<MockExternalCatalog>(catalog_name);
    }
    else if (boost::iequals(type, "hive") || boost::iequals(type, "glue"))
    {
        PlainConfigsPtr catalog_conf(new PlainConfigs());
        protoToConfig(catalog_model, *catalog_conf);
        catalog_conf->forEachKey([this](const std::string & key, const std::string & value) { LOG_TRACE(log, "{} - {}", key, value); });
        ret = std::make_shared<HiveExternalCatalog>(catalog_name, catalog_conf);
    }
    else
    {
        LOG_WARNING(log, "catalog type {} is implemented yet, model: {} ", type, catalog_model.DebugString());
        ret = nullptr;
    }
    return ret;
}
bool CnchExternalCatalogMgr::isCatalogExist(const std::string & catalog_name)
{
    std::vector<std::string> catalog_info;
    meta_proxy->getExternalCatalog(name_space, catalog_name, catalog_info);
    return !catalog_info.empty();
}


std::vector<std::pair<String, CatalogProperties>> CnchExternalCatalogMgr::listCatalog()
{
    std::vector<std::pair<String, CatalogProperties>> ret;
    auto it = meta_proxy->getAllExternalCatalogMeta(name_space);
    const auto prefix = DB::Catalog::MetastoreProxy::allExternalCatalogPrefix(name_space);
    while (it->next())
    {
        Protos::DataModelCatalog catalog_model;
        catalog_model.ParseFromString(it->value());
        CatalogProperties property{.create_time = static_cast<int64_t>(catalog_model.commit_time())};
        ret.emplace_back(DB::Catalog::unescapeString(it->key().substr(prefix.size())), std::move(property));
    }
    return ret;
}

std::optional<String> CnchExternalCatalogMgr::getCatalogCreateQuery(const std::string & catalog_name)
{
    std::vector<std::string> catalog_info;
    meta_proxy->getExternalCatalog(name_space, catalog_name, catalog_info);
    if (catalog_info.empty())
    {
        LOG_WARNING(log, "The catalog " + catalog_name + " dose not exsit.");
        return std::nullopt;
    }
    assert(catalog_info.size() == 1);
    const std::string & bin = catalog_info[0];
    Protos::DataModelCatalog catalog_model;
    catalog_model.ParseFromString(bin);
    WriteBufferFromOwnString wb;
    wb << "CREATE EXTERNAL CATALOG " << '`' << catalog_name << '`' << " PROPERTIES \n";
    auto number_of_property = catalog_model.properties().size();
    for (const auto & property : catalog_model.properties())
    {
        --number_of_property;
        wb << property.key() << "=" << DB::quote << property.value() << ((number_of_property > 0) ? "," : "") << '\n';
    }
    return {wb.str()};
}

bool CnchExternalCatalogMgr::dropExternalCatalog(const std::string & catalog_name)
{
    // {
    //     std::unique_lock lock(mu);
    //     catalogs.erase(catalog_name);
    // }
    Protos::DataModelCatalog catalog_model;
    catalog_model.set_name(catalog_name);
    meta_proxy->dropExternalCatalog(name_space, catalog_model);
    return true;
}

bool CnchExternalCatalogMgr::alterExternalCatalog(
    [[maybe_unused]] const std::string & catalog_name, [[maybe_unused]] PlainConfigs * changes)
{
    return false;
}
}
