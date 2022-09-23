#include <gtest/gtest.h>
#include <Interpreters/ExternalLoaderCnchCatalogRepository.h>

namespace UnitTestExternalLoader
{

using namespace DB;

TEST(TestExternalLoaderCnchCatalogRepository, TestParseStorageID)
{
    {
        StorageID storage_id = ExternalLoaderCnchCatalogRepository::parseStorageID("a.b");
        EXPECT_EQ(storage_id.getDatabaseName(), "a");
        EXPECT_EQ(storage_id.getTableName(), "b");
    }

    {
        StorageID storage_id = ExternalLoaderCnchCatalogRepository::parseStorageID("`\\`a`.`a.b`");
        EXPECT_EQ(storage_id.getDatabaseName(), "`a");
        EXPECT_EQ(storage_id.getTableName(), "a.b");
    }

    {
        StorageID storage_id = ExternalLoaderCnchCatalogRepository::parseStorageID("```a`.`\\`b`");
        EXPECT_EQ(storage_id.getDatabaseName(), "`a");
        EXPECT_EQ(storage_id.getTableName(), "`b");
    }

    EXPECT_THROW(ExternalLoaderCnchCatalogRepository::parseStorageID(""), Exception);
    EXPECT_THROW(ExternalLoaderCnchCatalogRepository::parseStorageID("``a.b"), Exception);
    EXPECT_THROW(ExternalLoaderCnchCatalogRepository::parseStorageID(".b"), Exception);
    EXPECT_THROW(ExternalLoaderCnchCatalogRepository::parseStorageID("a.``b"), Exception);
    EXPECT_THROW(ExternalLoaderCnchCatalogRepository::parseStorageID("a."), Exception);
    EXPECT_THROW(ExternalLoaderCnchCatalogRepository::parseStorageID("```a`."), Exception);
}

}
