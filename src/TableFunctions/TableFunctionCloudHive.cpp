// #include "TableFunctions/TableFunctionCloudHive.h"
// #if USE_HIVE

// #include "Disks/DiskLocal.h"
// #include "Interpreters/StorageID.h"
// #include "Storages/Hive/CnchHiveSettings.h"
// #include "Storages/Hive/DirectoryLister.h"
// #include "Storages/Hive/HivePartition.h"
// #include "Storages/Hive/StorageCloudHive.h"
// #include "Storages/StorageInMemoryMetadata.h"
// #include "TableFunctions/TableFunctionFactory.h"

// namespace DB
// {
// namespace ErrorCodes
// {
//     extern const int UNKNOWN_STORAGE;
// }

// StoragePtr TableFunctionCloudHive::getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const
// {
//     StorageInMemoryMetadata metadata;
//     metadata.setColumns(columns);
//     auto settings = std::make_shared<CnchHiveSettings>();
//     auto storage = std::make_shared<StorageCloudHive>(StorageID(getDatabaseName(), table_name), metadata, global_context, settings);

//     /// prepare hive file
//     auto format = IHiveFile::fromFormatName(arguments.format_name);
//     DiskPtr disk;
//     try
//     {
//         disk = getDiskFromURI(arguments.url, global_context);
//     }
//     catch (Exception & e)
//     {
//         String scheme = Poco::URI(arguments.url).getScheme();
//         if (e.code() == ErrorCodes::UNKNOWN_STORAGE 
//             && (scheme.empty() || scheme == "file"))
//         {
//             disk = std::make_shared<DiskLocal>("hive_disk", "/", 0);
//         }
//         else
//             throw;
//     }
//     auto path = Poco::URI(arguments.url).getPath();
//     auto hive_file = IHiveFile::create(format, path, 0, disk, std::make_shared<HivePartition>());

//     storage->loadHiveFiles({std::move(hive_file)});
//     return storage;
// }

// void registerTableFunctionCloudHive(TableFunctionFactory & factory)
// {
//     factory.registerFunction<TableFunctionCloudHive>();
// }

// }

// #endif
