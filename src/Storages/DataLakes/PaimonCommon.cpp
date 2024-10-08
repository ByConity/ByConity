#include "PaimonCommon.h"

#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/Hive/Metastore/HiveMetastore.h>
#include <consul/bridge.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int JNI_ERROR;
}

#define JNI_INVOKE(...) \
    initJNIMetaClient(); \
    LOG_TIMEUSAGE(); \
    SET_JNI_CONTEXT(); \
    try \
    { \
        return ([&]() { __VA_ARGS__; })(); \
    } \
    catch (...) \
    { \
        throw Exception(ErrorCodes::JNI_ERROR, "Failed to call JNI method, error: {}", getCurrentExceptionMessage(true)); \
    }


PaimonCatalogClient::PaimonCatalogClient(ContextPtr context_, CnchHiveSettingsPtr storage_settings_)
    : WithContext(context_), storage_settings(std::move(storage_settings_))
{
}

std::vector<String> PaimonCatalogClient::listDatabases()
{
    JNI_INVOKE(return jni_client->listDatabases();)
}

std::vector<String> PaimonCatalogClient::listTables(const String & database)
{
    JNI_INVOKE(return jni_client->listTables(database);)
}

bool PaimonCatalogClient::isTableExist(const String & database, const String & table)
{
    JNI_INVOKE(return jni_client->isTableExist(database, table);)
}

Protos::Paimon::Schema PaimonCatalogClient::getPaimonSchema(const String & database, const String & table)
{
    JNI_INVOKE({
        std::map<String, String> params;
        params[paimon_utils::PARAMS_KEY_DATABASE] = database;
        params[paimon_utils::PARAMS_KEY_TABLE] = table;
        auto proto_content = jni_client->getPaimonSchema(params);
        Protos::Paimon::Schema schema;
        schema.ParseFromString(proto_content);
        return schema;
    };)
}

PaimonScanInfo PaimonCatalogClient::getScanInfo(
    const String & database, const String & table, const std::vector<String> & required_fields, const std::optional<String> & rpn_predicate)
{
    JNI_INVOKE({
        std::map<String, String> params;
        params[paimon_utils::PARAMS_KEY_DATABASE] = database;
        params[paimon_utils::PARAMS_KEY_TABLE] = table;
        params[paimon_utils::PARAMS_KEY_REQUIRED_FIELDS] = paimon_utils::concat(required_fields);
        if (rpn_predicate.has_value())
        {
            params[paimon_utils::PARAMS_KEY_RPN_PREDICATE] = rpn_predicate.value();
        }
        auto [encoded_table, encoded_predicate, encoded_splits] = jni_client->getPaimonScanInfo(params);
        return PaimonScanInfo{std::move(encoded_table), std::move(encoded_predicate), std::move(encoded_splits)};
    };)
}

void PaimonCatalogClient::checkOrConvert(const String & database, const String & table, StorageInMemoryMetadata & metadata)
{
    paimon_utils::PaimonSchemaConverter converter(getContext(), storage_settings, getPaimonSchema(database, table));
    if (!metadata.columns.empty())
        converter.check(metadata);
    else
        converter.convert(metadata);
}


void PaimonCatalogClient::initJNIMetaClient()
{
    if (jni_client)
        return;

    LOG_TIMEUSAGE();
    try
    {
        jni_client = std::make_shared<JNIMetaClient>(
            paimon_utils::PAIMON_CLASS_FACTORY_CLASS, paimon_utils::PAIMON_CLIENT_CLASS, paimon_utils::serializeJson(buildCatalogParams()));
    }
    catch (...)
    {
        throw Exception(ErrorCodes::JNI_ERROR, "Failed to create JNIMetaClient, error: {}", getCurrentExceptionMessage(true));
    }
}

PaimonHiveCatalogClient::PaimonHiveCatalogClient(
    const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & metastore_url_)
    : PaimonCatalogClient(context_, storage_settings_), metastore_url(metastore_url_), warehouse({})
{
    parseMetastoreUrl();
    auto hive_metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(metastore_url, storage_settings);
    try
    {
        Strings all_databases = hive_metastore_client->getAllDatabases();
        if (all_databases.empty())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "No database found in hive metastore");
        String db_name = all_databases.front();
        auto database = hive_metastore_client->getDatabase(db_name);
        if (!database)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Failed to get database '{}'", db_name);
        String location = database->locationUri;
        auto pos = location.find_last_of('/');
        const_cast<String &>(warehouse) = location.substr(0, pos);

        LOG_DEBUG(log, "metastore_url: {}, warehouse: {}, sample_database: {}", metastore_url, warehouse, db_name);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to start up hive warehouse caused by {}", getCurrentExceptionMessage(true));
    }
}

void PaimonHiveCatalogClient::parseMetastoreUrl()
{
    const Poco::URI original_url(metastore_url);
    const auto & host = original_url.getHost();
    auto port = original_url.getPort();

    if (host.empty() || port == 0)
    {
        auto endpoint = paimon_utils::lookup(metastore_url);
        const_cast<String &>(metastore_url) = fmt::format("thrift://{}:{}", endpoint.host, endpoint.port);
    }
    LOG_DEBUG(log, "original metastore_url: {}, final metastore_url: {}", original_url.toString(), metastore_url);
}

Poco::JSON::Object PaimonHiveCatalogClient::buildCatalogParams()
{
    Poco::JSON::Object json;
    json.set(paimon_utils::PARAMS_KEY_METASTORE_TYPE, paimon_utils::METASTORE_TYPE_HIVE);
    json.set(paimon_utils::PARAMS_KEY_URI, metastore_url);
    json.set(paimon_utils::PARAMS_KEY_WAREHOUSE, warehouse);
    return json;
}

PaimonHDFSCatalogClient::PaimonHDFSCatalogClient(
    const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & warehouse_)
    : PaimonCatalogClient(context_, storage_settings_), warehouse(warehouse_)

{
    // only for internal use
    // HDFSConfigManager::instance().tryUpdate(warehouse);
    // LOG_DEBUG(log, "warehouse: {}", warehouse);
}

Poco::JSON::Object PaimonHDFSCatalogClient::buildCatalogParams()
{
    Poco::JSON::Object json;
    json.set(paimon_utils::PARAMS_KEY_METASTORE_TYPE, paimon_utils::METASTORE_TYPE_FILESYSTEM);
    json.set(paimon_utils::PARAMS_KEY_FILESYSTEM_TYPE, paimon_utils::FILESYSTEM_TYPE_HDFS);
    json.set(paimon_utils::PARAMS_KEY_WAREHOUSE, warehouse);
    return json;
}

PaimonLocalFilesystemCatalogClient::PaimonLocalFilesystemCatalogClient(
    const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & path_)
    : PaimonCatalogClient(context_, storage_settings_), path(path_)

{
    LOG_DEBUG(log, "path: {}", path);
}

Poco::JSON::Object PaimonLocalFilesystemCatalogClient::buildCatalogParams()
{
    Poco::JSON::Object json;
    json.set(paimon_utils::PARAMS_KEY_METASTORE_TYPE, paimon_utils::METASTORE_TYPE_FILESYSTEM);
    json.set(paimon_utils::PARAMS_KEY_FILESYSTEM_TYPE, paimon_utils::FILESYSTEM_TYPE_LOCAL);
    json.set(paimon_utils::PARAMS_KEY_PATH, path);
    return json;
}

PaimonS3CatalogClient::PaimonS3CatalogClient(const ContextPtr & context_, CnchHiveSettingsPtr storage_settings_, const String & warehouse_)
    : PaimonCatalogClient(context_, storage_settings_), warehouse(warehouse_)
{
    LOG_DEBUG(log, "warehouse: {}", warehouse);
}

Poco::JSON::Object PaimonS3CatalogClient::buildCatalogParams()
{
    Poco::JSON::Object json;
    json.set(paimon_utils::PARAMS_KEY_METASTORE_TYPE, paimon_utils::METASTORE_TYPE_FILESYSTEM);
    json.set(paimon_utils::PARAMS_KEY_FILESYSTEM_TYPE, paimon_utils::FILESYSTEM_TYPE_S3);
    json.set(paimon_utils::PARAMS_KEY_S3_ENDPOINT_REGION, storage_settings->region.value);
    json.set(paimon_utils::PARAMS_KEY_S3_ENDPOINT, storage_settings->endpoint.value);
    json.set(paimon_utils::PARAMS_KEY_S3_ACCESS_KEY, storage_settings->ak_id.value);
    json.set(paimon_utils::PARAMS_KEY_S3_SECRET_KEY, storage_settings->ak_secret.value);
    json.set(paimon_utils::PARAMS_KEY_S3_PATH_STYLE_ACCESS, storage_settings->s3_use_virtual_hosted_style.value);
    json.set(paimon_utils::PARAMS_KEY_WAREHOUSE, warehouse);

    if (!storage_settings->s3_extra_options.value.empty())
    {
        std::vector<String> s3_extra_option_pairs = CnchHiveSettings::splitStr(storage_settings->s3_extra_options.value, ",");
        for (const auto & pair : s3_extra_option_pairs)
        {
            auto kv = CnchHiveSettings::splitStr(pair, "=");
            if (kv.size() != 2)
                throw Exception("Invalid s3 extra option: " + pair, ErrorCodes::UNKNOWN_EXCEPTION);
            json.set(kv[0], kv[1]);
        }
    }
    return json;
}


namespace paimon_utils
{
    cpputil::consul::ServiceEndpoint lookup(const String & domain)
    {
        std::vector<cpputil::consul::ServiceEndpoint> endpoints;
        int retry = 0;
        do
        {
            if (retry++ > 2)
                throw Exception("No available nnproxy " + domain, ErrorCodes::NETWORK_ERROR);
            endpoints = cpputil::consul::lookup_name(domain);
        } while (endpoints.empty());

        std::vector<cpputil::consul::ServiceEndpoint> sample;
        std::sample(endpoints.begin(), endpoints.end(), std::back_inserter(sample), 1, std::mt19937{std::random_device{}()});
        return endpoints.back();
    }

    String concat(const std::vector<String> & items)
    {
        std::stringstream ss;
        for (size_t i = 0; i < items.size(); ++i)
        {
            ss << items[i];
            if (i != items.size() - 1)
                ss << ",";
        }
        return ss.str();
    }

    String serializeJson(const Poco::JSON::Object & json)
    {
        std::stringstream buf;
        Poco::JSON::Stringifier::stringify(json, buf);
        return buf.str();
    }

    std::optional<String> Predicate2RPNConverter::convert(ASTPtr & node)
    {
        Predicate2RPNContext context;
        Predicate2RPNConverter visitor;
        ASTVisitorUtil::accept(node, visitor, context);
        if (context.has_exception)
        {
            return std::nullopt;
        }
        return context.buffer.str();
    }

    void Predicate2RPNConverter::visitChildNode(ASTPtr & node, Predicate2RPNContext & context)
    {
        for (ASTPtr & child : node->children)
        {
            ASTVisitorUtil::accept(child, *this, context);
        }
    }

    void Predicate2RPNConverter::visitNode(ASTPtr & node, Predicate2RPNContext & context)
    {
        LOG_ERROR(log, "unsupported node type: {}", toString(node->getType()));
        context.has_exception = true;
    }

    void Predicate2RPNConverter::visitASTExpressionList(ASTPtr & node, Predicate2RPNContext & context)
    {
        visitChildNode(node, context);
    }

    void Predicate2RPNConverter::visitASTFunction(ASTPtr & node, Predicate2RPNContext & context)
    {
        const auto * func = node->as<ASTFunction>();

        writeString("@function(", context.buffer);
        writeString(func->name, context.buffer);
        writeString(",", context.buffer);
        if (func->children.size() == 1)
        {
            if (auto * expr_list = func->children[0]->as<ASTExpressionList>(); expr_list)
            {
                writeString(std::to_string(expr_list->children.size()), context.buffer);
            }
            else
            {
                writeString(std::to_string(func->children.size()), context.buffer);
            }
        }
        else
        {
            writeString(std::to_string(func->children.size()), context.buffer);
        }
        writeString(")", context.buffer);

        visitChildNode(node, context);
    }

    void Predicate2RPNConverter::visitASTIdentifier(ASTPtr & node, Predicate2RPNContext & context)
    {
        const auto * identifier = node->as<ASTIdentifier>();

        writeString("@identifier(", context.buffer);
        writeString(identifier->name(), context.buffer);
        writeString(")", context.buffer);
    }

    void Predicate2RPNConverter::visitASTLiteral(ASTPtr & node, Predicate2RPNContext & context)
    {
        const auto * literal = node->as<ASTLiteral>();

        writeString("@literal(", context.buffer);
        writeString(literal->value.dump(), context.buffer);
        writeString(")", context.buffer);
    }

    PaimonSchemaConverter::PaimonSchemaConverter(ContextPtr context_, CnchHiveSettingsPtr storage_settings_, Protos::Paimon::Schema schema_)
        : WithContext(context_), storage_settings(storage_settings_), schema(std::move(schema_))
    {
    }

    ASTCreateQuery PaimonSchemaConverter::createQueryAST(
        PaimonCatalogClientPtr catalog_client, const String & database, const String & database_in_paimon, const String & table) const
    {
        StorageInMemoryMetadata metadata;
        convert(metadata);
        ASTCreateQuery create_query;
        create_query.database = database;
        create_query.table = table;
        // craete_ast.catalog will be set explicitly .
        const auto & name_and_types = metadata.getColumns().getAll();
        std::string columns_str;
        {
            WriteBufferFromString wb(columns_str);
            size_t count = 0;
            for (auto it = name_and_types.begin(); it != name_and_types.end(); ++it, ++count)
            {
                writeBackQuotedString(it->name, wb);
                writeChar(' ', wb);
                writeString(it->type->getName(), wb);
                if (count != (name_and_types.size() - 1))
                {
                    writeChar(',', wb);
                }
            }
        }
        LOG_TRACE(log, "columns list {}", columns_str);
        ParserTablePropertiesDeclarationList parser;
        ASTPtr columns_list_raw = parseQuery(parser, columns_str, "columns declaration list", 262144, 100);
        create_query.set(create_query.columns_list, columns_list_raw);

        auto storage = std::make_shared<ASTStorage>();
        create_query.set(create_query.storage, storage);

        auto engine = std::make_shared<ASTFunction>();
        {
            engine->name = "CnchPaimon";
            engine->arguments = std::make_shared<ASTExpressionList>();
            engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(catalog_client->engineArgMetastoreType()));
            engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(catalog_client->engineArgUri()));
            engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(database_in_paimon));
            engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(table));
        }
        create_query.storage->set(create_query.storage->engine, engine);

        auto partition_def = buildPartitionDef();

        create_query.storage->set(create_query.storage->partition_by, partition_def);
        return create_query;
    }

    void PaimonSchemaConverter::convert(StorageInMemoryMetadata & metadata) const
    {
        ColumnsDescription columns;

        // Convert all columns
        for (const auto & field : schema.fields())
        {
            try
            {
                columns.add({field.name(), paimonType2CHType(field.type())});
            }
            catch (Exception & e)
            {
                if (storage_settings->enable_schema_covert_fault_tolerance)
                {
                    LOG_WARNING(log, "Failed to convert field '{}', error: {}", field.name(), e.displayText());
                }
                else
                {
                    throw;
                }
            }
        }

        // Convert partition keys
        auto partition_def = buildPartitionDef();

        metadata.setColumns(columns);
        if (partition_def)
        {
            KeyDescription partition_key = KeyDescription::getKeyFromAST(partition_def, columns, getContext());
            metadata.partition_key = partition_key;
        }
    }

    void PaimonSchemaConverter::check(const StorageInMemoryMetadata & metadata) const
    {
        const auto & columns = metadata.columns;
        std::unordered_map<String, size_t> field_name_2_idx;
        for (Int32 i = 0; i < schema.fields_size(); i++)
            field_name_2_idx[schema.fields(i).name()] = i;

        for (const auto & column : columns)
        {
            auto it = field_name_2_idx.find(column.name);
            if (it == field_name_2_idx.end())
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, fmt::format("Field '{}' not found in Paimon schema", column.name));

            DataTypePtr expected_type = paimonType2CHType(schema.fields(it->second).type());
            if (!expected_type->equals(*column.type))
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    fmt::format("Field '{}' type mismatch: {} != {}", column.name, column.type->getName(), expected_type->getName()));
        }

        if (metadata.isPartitionKeyDefined())
        {
            ASTPtr partition_def = metadata.getPartitionKeyAST();
            if (const auto * identifier = partition_def->as<ASTIdentifier>(); identifier)
            {
                if (schema.partition_keys_size() != 1 || schema.partition_keys(0) != identifier->name())
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Partition key mismatch");
            }
            else if (const auto * partition_func = partition_def->as<ASTFunction>(); partition_func)
            {
                if (partition_func->name != "tuple")
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Partition key mismatch");
                if (static_cast<size_t>(schema.partition_keys_size()) != partition_func->arguments->children.size())
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Partition key mismatch");
                for (size_t i = 0; i < partition_func->arguments->children.size(); i++)
                {
                    const auto * item_identifier = partition_func->arguments->children[i]->as<ASTIdentifier>();
                    if (!item_identifier || schema.partition_keys(i) != item_identifier->name())
                        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Partition key mismatch");
                }
            }
        }
    }

    ASTPtr PaimonSchemaConverter::buildPartitionDef() const
    {
        ASTPtr partition_def = nullptr;
        if (schema.partition_keys_size() == 1)
        {
            partition_def = std::make_shared<ASTIdentifier>(schema.partition_keys(0));
        }
        else if (schema.partition_keys_size() > 1)
        {
            auto partition_func = std::make_shared<ASTFunction>();
            partition_func->name = "tuple";
            partition_func->arguments = std::make_shared<ASTExpressionList>();
            for (const auto & field : schema.partition_keys())
            {
                partition_func->arguments->children.push_back(std::make_shared<ASTIdentifier>(field));
            }
            partition_func->children.push_back(partition_func->arguments);
            partition_def = partition_func;
        }
        return partition_def;
    }

    DataTypePtr PaimonSchemaConverter::paimonType2CHType(Protos::Paimon::Type type)
    {
        DataTypePtr data_type;
        if (type.has_arraytype())
        {
            data_type = std::make_shared<DataTypeArray>(paimonType2CHType(type.arraytype().elementtype()));
        }
        else if (type.has_biginttype())
        {
            data_type = std::make_shared<DataTypeInt64>();
        }
        else if (type.has_binarytype())
        {
            data_type = std::make_shared<DataTypeFixedString>(type.binarytype().length());
        }
        else if (type.has_booleantype())
        {
            data_type = std::make_shared<DataTypeUInt8>();
        }
        else if (type.has_chartype())
        {
            data_type = std::make_shared<DataTypeFixedString>(type.chartype().length());
        }
        else if (type.has_datetype())
        {
            data_type = std::make_shared<DataTypeDate32>();
        }
        else if (type.has_decimaltype())
        {
            data_type = createDecimal<DataTypeDecimal>(type.decimaltype().precision(), type.decimaltype().scale());
        }
        else if (type.has_doubletype())
        {
            data_type = std::make_shared<DataTypeFloat64>();
        }
        else if (type.has_floattype())
        {
            data_type = std::make_shared<DataTypeFloat32>();
        }
        else if (type.has_inttype())
        {
            data_type = std::make_shared<DataTypeInt32>();
        }
        else if (type.has_localzonedtimestamptype())
        {
            data_type = std::make_shared<DataTypeDateTime64>(type.localzonedtimestamptype().precision());
        }
        else if (type.has_maptype())
        {
            data_type
                = std::make_shared<DataTypeMap>(paimonType2CHType(type.maptype().keytype()), paimonType2CHType(type.maptype().valuetype()));
        }
        else if (type.has_rowtype())
        {
            DataTypes elements;
            Strings names;
            type.rowtype().fields();
            for (const auto & field : type.rowtype().fields())
            {
                names.emplace_back(field.name());
                elements.emplace_back(paimonType2CHType(field.type()));
            }
            data_type = std::make_shared<DataTypeTuple>(elements, names);
        }
        else if (type.has_smallinttype())
        {
            data_type = std::make_shared<DataTypeInt16>();
        }
        else if (type.has_timestamptype())
        {
            data_type = std::make_shared<DataTypeDateTime64>(type.timestamptype().precision());
        }
        else if (type.has_timetype())
        {
            if (type.timetype().precision() != 0)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Paimon's time type with precision is not supported");
            data_type = std::make_shared<DataTypeDateTime>();
        }
        else if (type.has_tinyinttype())
        {
            data_type = std::make_shared<DataTypeInt8>();
        }
        else if (type.has_varbinarytype())
        {
            data_type = std::make_shared<DataTypeString>();
        }
        else if (type.has_varchartype())
        {
            data_type = std::make_shared<DataTypeString>();
        }
        else
        {
            __builtin_unreachable();
        }

        if (type.is_nullable())
            return makeNullable(data_type);
        else
            return data_type;
    }
}

}

#endif
