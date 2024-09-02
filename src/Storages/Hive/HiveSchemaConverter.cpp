#include "Storages/Hive/HiveSchemaConverter.h"

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <boost/algorithm/string/split.hpp>
#include <Poco/Logger.h>
#include "common/logger_useful.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeMap.h"
#include "DataTypes/DataTypeDate.h"
#include "DataTypes/DataTypeDate32.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/DataTypeDateTime64.h"
#include "DataTypes/DataTypeDecimalBase.h"
#include "DataTypes/DataTypeFixedString.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypeTuple.h"
#include "DataTypes/DataTypesDecimal.h"
#include "Interpreters/Context.h"
#include "Interpreters/InterpreterCreateQuery.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTClusterByElement.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/formatAST.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/KeyDescription.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "hivemetastore/hive_metastore_types.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}
static std::pair<String, String> getKeywordWithInnerType(const String & hive_type_name)
{
    size_t pos = hive_type_name.find('<');
    if (pos != String::npos)
    {
        size_t right_pos = hive_type_name.find('>');
        size_t len = right_pos - pos - 1;
        return {hive_type_name.substr(0, pos), hive_type_name.substr(pos + 1, len)};
    }

    pos = hive_type_name.find('(');
    if (pos != String::npos)
    {
        size_t right_pos = hive_type_name.find(')');
        size_t len = right_pos - pos - 1;
        return {hive_type_name.substr(0, pos), hive_type_name.substr(pos + 1, len)};
    }
    return {hive_type_name, ""};
}

DataTypePtr HiveSchemaConverter::hiveTypeToCHType(const String & hive_type, bool make_columns_nullable)
{
    static const std::unordered_map<String, std::shared_ptr<IDataType>> base_type_mapping = {
        {"tinyint", std::make_shared<DataTypeInt8>()},
        {"smallint", std::make_shared<DataTypeInt16>()},
        {"bigint", std::make_shared<DataTypeInt64>()},
        {"int", std::make_shared<DataTypeInt32>()},
        {"integer", std::make_shared<DataTypeInt32>()},
        {"float", std::make_shared<DataTypeFloat32>()},
        {"double", std::make_shared<DataTypeFloat64>()},
        {"string", std::make_shared<DataTypeString>()},
        {"varchar",
         std::make_shared<DataTypeString>()}, // varchar and string are both treated as string, while char will be treated as fixed string.
        {"boolean", std::make_shared<DataTypeUInt8>()},
        {"binary", std::make_shared<DataTypeString>()},
        {"date", std::make_shared<DataTypeDate32>()},
        {"timestamp", std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale)}
    };

    DataTypePtr data_type;

    auto [type_keyword, inner] = getKeywordWithInnerType(hive_type);

    if (auto it = base_type_mapping.find(type_keyword); it != base_type_mapping.end())
    {
        data_type = it->second;
    }

    if (type_keyword == "array")
    {
        auto inner_type = hiveTypeToCHType(inner, make_columns_nullable);
        if (inner_type)
            data_type = std::make_shared<DataTypeArray>(inner_type);
    }
    else if (type_keyword == "char")
    {
        auto n = std::stoi(inner);
        if (n > 0 && n < MAX_FIXEDSTRING_SIZE)
        {
            data_type = std::make_shared<DataTypeFixedString>(n);
        }
        else
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Unable to create fixed string type with length {}", n);
        }
    }
    else if (type_keyword == "map")
    {
        Strings res;
        boost::split(res, inner, boost::is_any_of(","), boost::token_compress_on);
        // ck type key and value is not nullable
        auto key_type = hiveTypeToCHType(res.at(0), false);
        auto value_type = hiveTypeToCHType(res.at(1), false);
        data_type = std::make_shared<DataTypeMap>(key_type, value_type);
    }
    else if (type_keyword == "decimal")
    {
        Strings res;
        boost::split(res, inner, boost::is_any_of(","), boost::token_compress_on);
        auto precision = std::stoi(res.at(0));
        auto scale = std::stoi(res.at(1));
        data_type = createDecimal<DataTypeDecimal>(precision, scale);
    }
    else if (type_keyword == "struct")
    {
        // struct<F1:TYPE1,F2:TYPE2,F3:TYPE3..>
        Strings components;
        boost::split(components, inner, boost::is_any_of(","), boost::token_compress_on);
        DataTypes child_types;
        Strings child_names;
        for (auto & component : components) {
            Strings name_and_types;
            boost::split(name_and_types, component, boost::is_any_of(":"), boost::token_compress_on);
            child_names.emplace_back(name_and_types.at(0));
            child_types.emplace_back(hiveTypeToCHType(name_and_types.at(1), true));
        }

        data_type = std::make_shared<DataTypeTuple>(child_types, child_names);
    }

    if (make_columns_nullable && data_type && data_type->canBeInsideNullable())
    {
        data_type = makeNullable(data_type);
    }

    return data_type;
}

HiveSchemaConverter::HiveSchemaConverter(ContextPtr context_, std::shared_ptr<Apache::Hadoop::Hive::Table> hive_table_)
    : WithContext(context_), hive_table(std::move(hive_table_))
{
}

void HiveSchemaConverter::convert(StorageInMemoryMetadata & metadata) const
{
    ColumnsDescription columns;
    auto addColumn = [&](const Apache::Hadoop::Hive::FieldSchema & hive_field, bool make_column_nullable) {
        // bool make_columns_nullable = getContext()->getSettingsRef().data_type_default_nullable;
        DataTypePtr ch_type = hiveTypeToCHType(hive_field.type, make_column_nullable);
        if (ch_type)
            columns.add(ColumnDescription(hive_field.name, ch_type));
        else
            LOG_WARNING(log, "Unsupport type {} for column {}, ignore the column", hive_field.type, hive_field.name);
    };

    for (const auto & hive_field : hive_table->sd.cols)
    {
        addColumn(hive_field, true);
    }

    {
        auto partition_def = std::make_shared<ASTFunction>();
        partition_def->name = "tuple";
        partition_def->arguments = std::make_shared<ASTExpressionList>();
        for (const auto & hive_field : hive_table->partitionKeys)
        {
            if (!columns.has(hive_field.name))
                addColumn(hive_field, false);

            auto col = std::make_shared<ASTIdentifier>(hive_field.name);
            partition_def->arguments->children.emplace_back(col);
        }

        partition_def->children.push_back(partition_def->arguments);
        metadata.partition_key = KeyDescription::getKeyFromAST(partition_def, columns, getContext());
    }

    /// has bucket key
    if (!hive_table->sd.bucketCols.empty())
    {
        ASTs args;
        args.reserve(hive_table->sd.bucketCols.size());
        for (const auto & bucket_col_name : hive_table->sd.bucketCols)
        {
            if (!columns.has(bucket_col_name))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "hive bucket col {} not found in schema", bucket_col_name);

            args.push_back(std::make_shared<ASTIdentifier>(bucket_col_name));
        }
        auto func_hash = makeASTFunction("javaHash", args);
        auto bucket_num = std::make_shared<ASTLiteral>(hive_table->sd.numBuckets);
        auto func_mod = makeASTFunction("hiveModulo", ASTs{func_hash, bucket_num});
        auto cluster_key = std::make_shared<ASTClusterByElement>(func_mod, bucket_num, -1, false, false);
        metadata.cluster_by_key = KeyDescription::getClusterByKeyFromAST(cluster_key, columns, getContext());
    }

    metadata.setColumns(columns);
}

void HiveSchemaConverter::check(const StorageInMemoryMetadata & metadata) const
{
    const auto & columns = metadata.columns;
    std::unordered_map<String, String> hive_table_columns;
    {
        for (const auto & field : hive_table->sd.cols)
            hive_table_columns.emplace(field.name, field.type);

        for (const auto & field : hive_table->partitionKeys)
            hive_table_columns.emplace(field.name, field.type);
    }

    for (const auto & column : columns)
    {
        auto it = hive_table_columns.find(column.name);

        if (it != hive_table_columns.end())
        {
            DataTypePtr expected = hiveTypeToCHType(it->second, false);
            DataTypePtr actual
                = column.type->isNullable() ? static_cast<const DataTypeNullable &>(*column.type).getNestedType() : column.type;
            actual->equals(*expected);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Unable to find column {} in hive table {}.{}",
                column.name,
                hive_table->dbName,
                hive_table->tableName);
        }
    }
}

CloudTableBuilder::CloudTableBuilder() : create_query(std::make_shared<ASTCreateQuery>())
{
}

CloudTableBuilder & CloudTableBuilder::setMetadata(const StorageMetadataPtr & metadata)
{
    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(metadata->getColumns(), ParserSettings::CLICKHOUSE);
    create_query->set(create_query->columns_list, std::make_shared<ASTColumns>());
    create_query->set(create_query->columns_list->columns, new_columns);
    create_query->set(create_query->storage, std::make_shared<ASTStorage>());

    if (metadata->hasPartitionKey())
    {
        create_query->storage->set(create_query->storage->partition_by, metadata->getPartitionKeyAST());
    }

    if (metadata->hasSettingsChanges())
    {
        create_query->storage->set(create_query->storage->settings, metadata->getSettingsChanges());
    }

    return *this;
}

CloudTableBuilder & CloudTableBuilder::setCloudEngine(const String & cloudEngineName)
{
    if (!create_query->storage)
        create_query->set(create_query->storage, std::make_shared<ASTStorage>());

    auto engine = std::make_shared<ASTFunction>();
    {
        engine->name = cloudEngineName;
        engine->arguments = std::make_shared<ASTExpressionList>();
    }
    create_query->storage->set(create_query->storage->engine, engine);

    return *this;
}

CloudTableBuilder & CloudTableBuilder::setStorageID(const StorageID & storage_id)
{
    create_query->table = storage_id.table_name;
    create_query->database = storage_id.database_name;
    create_query->uuid = storage_id.uuid;
    return *this;
}

String CloudTableBuilder::build() const
{
    WriteBufferFromOwnString statement_buf;
    formatAST(*create_query, statement_buf, false);
    writeChar('\n', statement_buf);
    return statement_buf.str();
}

const String & CloudTableBuilder::cloudTableName() const
{
    return create_query->table;
}


ASTCreateQuery HiveSchemaConverter::createQueryAST(const std::string & catalog_name) const
{
    StorageInMemoryMetadata metadata;
    convert(metadata);
    ASTCreateQuery create_query;
    create_query.create = true;
    create_query.table = hive_table->tableName;
    create_query.database = hive_table->dbName;
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
        engine->name = "CnchHive";
        const auto & input_format = hive_table->sd.inputFormat;
        if (input_format == "org.apache.hudi.hadoop.HoodieParquetInputFormat")
        {
            engine->name = "CnchHudi";
        }
#if USE_JAVA_EXTENSIONS
        else if (input_format == "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat")
        {
            engine->name = "CnchHudi";
        }
#endif
        engine->arguments = std::make_shared<ASTExpressionList>();
        // We just fill a dummy str for psm field.
        engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(catalog_name));
        engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(hive_table->dbName));
        engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(hive_table->tableName));
    }
    create_query.storage->set(create_query.storage->engine, engine);

    auto partition_def = std::make_shared<ASTFunction>();
    {
        partition_def->name = "tuple";
        partition_def->arguments = std::make_shared<ASTExpressionList>();
        for (const auto & hive_field : hive_table->partitionKeys)
        {
            auto col = std::make_shared<ASTIdentifier>(hive_field.name);
            partition_def->arguments->children.emplace_back(col);
        }

        partition_def->children.push_back(partition_def->arguments);
    }
    create_query.storage->set(create_query.storage->partition_by, partition_def);
    return create_query;
}
}
