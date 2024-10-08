#pragma once

#include <Common/Logger.h>
#include <Common/config.h>
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <memory>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTVisitor.h>
#include <Storages/DataLakes/HiveFile/JNIArrowSource.h>
#include <Storages/DataLakes/JNIUtils.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <consul/discovery.h>
#include <jni/JNIArrowReader.h>
#include <jni/JNIMetaClient.h>
#include <paimon.pb.h>
#include <Poco/JSON/JSON.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <common/shared_ptr_helper.h>
#include <common/types.h>

namespace DB
{

class PaimonCatalogClient;
using PaimonCatalogClientPtr = std::shared_ptr<PaimonCatalogClient>;

struct PaimonScanInfo
{
    String encoded_table;
    std::optional<String> encoded_predicate;
    std::vector<String> encoded_splits;
};

class PaimonCatalogClient : WithContext
{
public:
    PaimonCatalogClient(ContextPtr context_, CnchHiveSettingsPtr storage_settings_);
    virtual ~PaimonCatalogClient() = default;
    virtual String engineArgMetastoreType() const = 0;
    virtual String engineArgUri() const = 0;
    std::vector<String> listDatabases();
    std::vector<String> listTables(const String & database);
    bool isTableExist(const String & database, const String & table);
    void checkOrConvert(const String & database, const String & table, StorageInMemoryMetadata & metadata);
    Protos::Paimon::Schema getPaimonSchema(const String & database, const String & table);
    PaimonScanInfo getScanInfo(
        const String & database,
        const String & table,
        const std::vector<String> & required_fields,
        const std::optional<String> & rpn_predicate);

protected:
    virtual Poco::JSON::Object buildCatalogParams() = 0;

private:
    void initJNIMetaClient();

protected:
    const CnchHiveSettingsPtr storage_settings;

    std::shared_ptr<JNIMetaClient> jni_client;

private:
    LoggerPtr log{getLogger("PaimonCatalogClient")};
};

class PaimonHiveCatalogClient final : public PaimonCatalogClient, public shared_ptr_helper<PaimonHiveCatalogClient>
{
public:
    PaimonHiveCatalogClient(const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & metastore_url_);
    String engineArgMetastoreType() const override { return "hive"; }
    String engineArgUri() const override { return metastore_url; }

protected:
    Poco::JSON::Object buildCatalogParams() override;

private:
    void parseMetastoreUrl();
    LoggerPtr log{getLogger("PaimonHiveCatalogClient")};

    const String metastore_url;
    const String warehouse;
};

class PaimonHDFSCatalogClient final : public PaimonCatalogClient, public shared_ptr_helper<PaimonHDFSCatalogClient>
{
public:
    PaimonHDFSCatalogClient(const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & warehouse_);
    String engineArgMetastoreType() const override { return "filesystem:HDFS"; }
    String engineArgUri() const override { return warehouse; }

protected:
    Poco::JSON::Object buildCatalogParams() override;

private:
    LoggerPtr log{getLogger("PaimonHDFSCatalogClient")};

    const String warehouse;
};

class PaimonLocalFilesystemCatalogClient final : public PaimonCatalogClient, public shared_ptr_helper<PaimonLocalFilesystemCatalogClient>
{
public:
    PaimonLocalFilesystemCatalogClient(const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & path_);
    String engineArgMetastoreType() const override { return "filesystem:LOCAL"; }
    String engineArgUri() const override { return path; }

protected:
    Poco::JSON::Object buildCatalogParams() override;

private:
    LoggerPtr log{getLogger("PaimonLocalFilesystemCatalogClient")};

    const String path;
};

class PaimonS3CatalogClient final : public PaimonCatalogClient, public shared_ptr_helper<PaimonS3CatalogClient>
{
public:
    PaimonS3CatalogClient(const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & warehouse_);
    String engineArgMetastoreType() const override { return "filesystem:S3"; }
    String engineArgUri() const override { return warehouse; }

protected:
    Poco::JSON::Object buildCatalogParams() override;

private:
    LoggerPtr log{getLogger("PaimonS3CatalogClient")};

    const String warehouse;
};


namespace paimon_utils
{
    static constexpr auto PAIMON_CLASS_FACTORY_CLASS = "org/byconity/paimon/PaimonClassFactory";
    static constexpr auto PAIMON_CLIENT_CLASS = "org/byconity/paimon/PaimonMetaClient";
    static constexpr auto PAIMON_ARROW_READER_CLASS = "org/byconity/paimon/reader/PaimonArrowReaderBuilder";

    static constexpr auto PARAMS_KEY_METASTORE_TYPE = "metastoreType";
    static constexpr auto PARAMS_KEY_FILESYSTEM_TYPE = "filesystemType";
    static constexpr auto PARAMS_KEY_URI = "uri";
    static constexpr auto PARAMS_KEY_WAREHOUSE = "warehouse";
    static constexpr auto PARAMS_KEY_PATH = "path";
    static constexpr auto PARAMS_KEY_S3_ENDPOINT_REGION = "s3.endpoint.region";
    static constexpr auto PARAMS_KEY_S3_ENDPOINT = "s3.endpoint";
    static constexpr auto PARAMS_KEY_S3_ACCESS_KEY = "s3.access-key";
    static constexpr auto PARAMS_KEY_S3_SECRET_KEY = "s3.secret-key";
    static constexpr auto PARAMS_KEY_S3_PATH_STYLE_ACCESS = "s3.path.style.access";
    static constexpr auto PARAMS_KEY_DATABASE = "database";
    static constexpr auto PARAMS_KEY_TABLE = "table";
    static constexpr auto PARAMS_KEY_RPN_PREDICATE = "rpn_predicate";
    static constexpr auto PARAMS_KEY_FETCH_SIZE = "fetch_size";
    static constexpr auto PARAMS_KEY_REQUIRED_FIELDS = "required_fields";
    static constexpr auto PARAMS_KEY_NESTED_FIELDS = "nested_fields";
    static constexpr auto PARAMS_KEY_ENCODED_TABLE = "encoded_table";
    static constexpr auto PARAMS_KEY_ENCODED_SPLITS = "encoded_splits";
    static constexpr auto PARAMS_KEY_ENCODED_PREDICATE = "encoded_predicate";

    static constexpr auto METASTORE_TYPE_HIVE = "hive";
    static constexpr auto METASTORE_TYPE_FILESYSTEM = "filesystem";
    static constexpr auto FILESYSTEM_TYPE_HDFS = "HDFS";
    static constexpr auto FILESYSTEM_TYPE_LOCAL = "LOCAL";
    static constexpr auto FILESYSTEM_TYPE_S3 = "S3";

    // Split a vector into num_groups groups, if some groups are empty, they will be ignored
    template <typename Iterator, typename Value = typename Iterator::value_type>
    std::vector<std::vector<Value>> splitEqually(const Iterator first, const Iterator last, size_t num_groups)
    {
        if (first >= last || num_groups == 0)
        {
            return {};
        }

        std::vector<std::vector<Value>> groups(num_groups);
        for (auto it = first; it != last; ++it)
        {
            groups[(it - first) % num_groups].emplace_back(std::move(*it));
        }

        const size_t size = std::distance(first, last);
        if (size < num_groups)
        {
            groups.resize(size);
        }
        return groups;
    }

    template <typename T>
    String getSizes(std::vector<std::vector<T>> groups)
    {
        std::vector<size_t> sizes;
        std::transform(
            groups.begin(), groups.end(), std::back_inserter(sizes), [](const std::vector<std::string> & group) { return group.size(); });
        return std::accumulate(sizes.begin(), sizes.end(), std::string(), [](const std::string & a, size_t b) {
            return a.empty() ? std::to_string(b) : a + "," + std::to_string(b);
        });
    }

    cpputil::consul::ServiceEndpoint lookup(const String & domain);
    String concat(const std::vector<String> & items);
    String serializeJson(const Poco::JSON::Object & json);

    struct Predicate2RPNContext
    {
        WriteBufferFromOwnString buffer;
        bool has_exception = false;
    };
    // Convert a predicate expression to Reverse Polish Notation, RPN
    class Predicate2RPNConverter : public ASTVisitor<void, Predicate2RPNContext>
    {
    public:
        static std::optional<String> convert(ASTPtr & node);

    protected:
        void visitNode(ASTPtr & node, Predicate2RPNContext & context) override;
        void visitASTExpressionList(ASTPtr & node, Predicate2RPNContext & context) override;
        void visitASTFunction(ASTPtr & node, Predicate2RPNContext & context) override;
        void visitASTIdentifier(ASTPtr & node, Predicate2RPNContext & context) override;
        void visitASTLiteral(ASTPtr & node, Predicate2RPNContext & context) override;

    private:
        void visitChildNode(ASTPtr & node, Predicate2RPNContext & context);

        LoggerPtr log{getLogger("Predicate2RPNConverter")};
    };

    class PaimonSchemaConverter : WithContext
    {
    public:
        PaimonSchemaConverter(ContextPtr context_, CnchHiveSettingsPtr storage_settings_, Protos::Paimon::Schema schema_);

        ASTCreateQuery createQueryAST(
            PaimonCatalogClientPtr catalog_client, const String & database, const String & database_in_paimon, const String & table) const;
        void convert(StorageInMemoryMetadata & metadata) const;
        void check(const StorageInMemoryMetadata & metadata) const;

    private:
        ASTPtr buildPartitionDef() const;
        static DataTypePtr paimonType2CHType(Protos::Paimon::Type type);

        LoggerPtr log{getLogger("PaimonSchemaConverter")};

        const CnchHiveSettingsPtr storage_settings;
        Protos::Paimon::Schema schema;
    };

}

}

#endif
