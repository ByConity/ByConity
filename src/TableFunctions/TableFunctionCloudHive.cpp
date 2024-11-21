#include "TableFunctions/TableFunctionCloudHive.h"
#if USE_HIVE

#include <Disks/DiskLocal.h>
#include <Interpreters/StorageID.h>
#include <Storages/DataLakes/ScanInfo/FileScanInfo.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/DirectoryLister.h>
#include <Storages/Hive/HivePartition.h>
#include <Storages/Hive/StorageCloudHive.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

StoragePtr
TableFunctionCloudHive::getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    auto settings = std::make_shared<CnchHiveSettings>(global_context->getCnchHiveSettings());
    /// here global_context maybe query_context
    /// anyway this table function is used for testing purpose;
    const auto & ctx_settings = global_context->getSettingsRef();
    settings->endpoint = ctx_settings.s3_endpoint;
    settings->region = ctx_settings.s3_region;
    settings->ak_id = ctx_settings.s3_access_key_id;
    settings->ak_secret = ctx_settings.s3_access_key_secret;

    auto storage = std::make_shared<StorageCloudHive>(StorageID(getDatabaseName(), table_name), metadata, global_context, settings);

    /// prepare hive file
    auto format = FileScanInfo::parseFormatTypeFromString(arguments.format_name);
    DiskPtr disk;
    try
    {
        disk = HiveUtil::getDiskFromURI(arguments.url, global_context, *settings);
    }
    catch (Exception & e)
    {
        String scheme = Poco::URI(arguments.url).getScheme();
        if (e.code() == ErrorCodes::UNKNOWN_STORAGE && (scheme.empty() || scheme == "file"))
        {
            disk = std::make_shared<DiskLocal>("hive_disk", "/", DiskStats{});
        }
        else
            throw;
    }
    String path = HiveUtil::getPath(arguments.url);
    size_t file_size = disk->getFileSize(path);
    LakeScanInfos lake_scan_infos;
    lake_scan_infos.push_back(FileScanInfo::create(ILakeScanInfo::StorageType::Hive, format, arguments.url, file_size, std::nullopt));

    storage->loadLakeScanInfos(lake_scan_infos);
    return storage;
}

void registerTableFunctionCloudHive(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCloudHive>();
}

}

#endif
