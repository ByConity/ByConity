#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionStringToString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeByteMap.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnByteMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Stopwatch.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <unordered_set>
#include <iostream>
#include <regex>
#include <common/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int CANNOT_CONVERT_TYPE;
}

/**
 * mapElement(map, key) is a function that allows you to retrieve a column from map
 * How to implement it depends on the storage model of map type
 *  - option 1: if map is simply serialized lob, and this function need to get the
 *    deserialized map type, and access element correspondingly
 *
 *  - option 2: if map is stored as expanded implicit column, and per key's value is
 *    stored as single file, this functions could be intepreted as implicited column
 *    ref, and more efficient.
 *
 * Option 2 will be used in TEA project, but we go to option 1 firstly for demo, and
 * debug end-to-end prototype.
 *
 *
 * mapKeys(map) is a function that retrieve keys array of a map column row.
 */

class FunctionMapElement : public IFunction
{
public:
    static constexpr auto name = "mapElement";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionMapElement>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const IDataType * map_col = arguments[0].type.get();
        const DataTypeByteMap * map = checkAndGetDataType<DataTypeByteMap>(map_col);

        if (!map)
            throw Exception("First argument for function " + getName() + " must be map.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (map->getKeyType()->getName() != arguments[1].type->getName())
            throw Exception("Second argument for function " + getName() + " does not match map key type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (map->valueTypeIsLC())
            return map->getValueType();

        return makeNullable(map->getValueType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
	{
        const ColumnByteMap * col_map = checkAndGetColumn<ColumnByteMap>(arguments[0].column.get());
        auto& col_sel = arguments[1].column;
        if (!col_map)
            throw Exception("Input column doesn't match", ErrorCodes::LOGICAL_ERROR);

        auto& keyColumn = col_map->getKey();
        auto& valueColumn = col_map->getValue();

        // fix result type for low cardinality
        auto col_res = [&]() {
            if (valueColumn.lowCardinality())
                return result_type->createColumn()->assumeMutable();
            else
                return makeNullable(valueColumn.cloneEmpty())->assumeMutable();
        }();

        auto & offsets = col_map->getOffsets();
        size_t size = offsets.size();

        ColumnByteMap::Offset src_prev_offset = 0;

        // TODO: below is generic implementation, and could be optimized.
        for (size_t i = 0; i < size; ++i)
        {
            bool found = false;
            for (size_t j = src_prev_offset; j < offsets[i]; ++j)
            {
                // locate if key match input
                if (keyColumn[j] == (*col_sel)[i])
                {
                    col_res->insert(valueColumn[j]);
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                col_res->insert(Null());
            }

            src_prev_offset = offsets[i];
        }

        return col_res;
	}
};

class FunctionMapKeys : public IFunction
{
public:
    static constexpr auto name = "mapKeys";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionMapKeys>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const IDataType * map_col = arguments[0].type.get();
        const DataTypeByteMap * map = checkAndGetDataType<DataTypeByteMap>(map_col);

        if (!map)
            throw Exception("First argument for function " + getName() + " must be map.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(map->getKeyType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnByteMap * col_map = checkAndGetColumn<ColumnByteMap>(arguments[0].column.get());
        if (!col_map)
            throw Exception("Input column doesn't match", ErrorCodes::LOGICAL_ERROR);

        auto col_res = ColumnArray::create(col_map->getKeyPtr(), col_map->getOffsetsPtr());
        return col_res;

    }
};

class FunctionMapValues : public IFunction
{
public:
    static constexpr auto name = "mapValues";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionMapValues>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const IDataType * map_col = arguments[0].type.get();
        const DataTypeByteMap * map = checkAndGetDataType<DataTypeByteMap>(map_col);

        if (!map)
            throw Exception("First argument for function " + getName() + " must be map.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(map->getValueType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnByteMap * col_map = checkAndGetColumn<ColumnByteMap>(arguments[0].column.get());
        if (!col_map)
            throw Exception("Input column doesn't match", ErrorCodes::LOGICAL_ERROR);

        auto col_res = ColumnArray::create(col_map->getValuePtr(), col_map->getOffsetsPtr());
        return col_res;

    }
};

class FunctionGetMapKeys : public IFunction
{
public:
    static constexpr auto name = "getMapKeys";

    static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionGetMapKeys>(context); }

    FunctionGetMapKeys(const ContextPtr & context_):context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4 && arguments.size() != 5)
            throw Exception("Function " + getName()
                            + " requires 3 or 4 or 5 parameters: db, table, column, [partition expression], [max execute time]. Passed "
                            + toString(arguments.size()),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        for (unsigned int i = 0; i < (arguments.size() == 5 ? 4 : arguments.size()); i++)
        {
            const IDataType * argument_type = arguments[i].type.get();
            const DataTypeString * argument = checkAndGetDataType<DataTypeString>(argument_type);
            if (!argument)
                throw Exception("Illegal column " + arguments[i].name + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }
        if (arguments.size() == 5)
        {
            bool ok = checkAndGetDataType<DataTypeUInt64>(arguments[4].type.get())
                      || checkAndGetDataType<DataTypeUInt32>(arguments[4].type.get())
                      || checkAndGetDataType<DataTypeUInt16>(arguments[4].type.get())
                      || checkAndGetDataType<DataTypeUInt8>(arguments[4].type.get());
            if (!ok)
                throw Exception("Illegal column " + arguments[4].name + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    StringRef getColumnStringValue(const ColumnPtr & column_ptr) const
    {
        return column_ptr->getDataAt(0);
    }

    ColumnPtr executeImpl([[maybe_unused]]const ColumnsWithTypeAndName & arguments, [[maybe_unused]]const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
		return nullptr;
#if 0  // TODO: need align with ClusterProxy::executeQuery

        using MapKeySet = std::unordered_set<String>;
        MapKeySet mapKeySet;

        bool has_partition_expression = false;
        std::regex partition_expression;
        StringRef db_name = getColumnStringValue(arguments[0].column);
        StringRef table_name = getColumnStringValue(arguments[1].column);
        StringRef column_name = getColumnStringValue(arguments[2].column);

        if (arguments.size() >= 4)
        {
            StringRef partition_expression_str = getColumnStringValue(arguments[3].column);
            if (partition_expression_str.size > 0)
            {
                has_partition_expression = true;
                partition_expression = std::regex(partition_expression_str.toString());
            }
        }

        UInt64 max_execute_time_in_second = 60;
        if (arguments.size() == 5)
        {
            const ColumnPtr & column_ptr = arguments[4].column;
            max_execute_time_in_second = column_ptr->get64(0);
        }

        auto col_res = ColumnArray::create(ColumnString::create());
        if (db_name.size > 0 && table_name.size > 0 && column_name.size > 0)
        {
            StoragePtr table = DatabaseCatalog::instance().getTable(db_name.toString(), table_name.toString());
            StorageDistributed * distributed_table = dynamic_cast<StorageDistributed *>(table.get());
            if (distributed_table)
            {
                ClusterPtr cluster = distributed_table->getCluster();
                std::string remote_db_name = distributed_table->getRemoteDatabaseName();
                std::string remote_table_name = distributed_table->getRemoteTableName();

				// TODO
                // SlowShardsDiagPtr slowDiag = nullptr;

                //if (!context.getSettings().disable_remote_stream_log)
                //{
                //    slowDiag = std::make_shared<SlowShardsDiag>(context, &Poco::Logger::get("getMapKeys"));
                //}

                String query = "SELECT " + getName() +
                               "('" + remote_db_name + "','" + remote_table_name + "','" +column_name.toString() + "','"
                               + (has_partition_expression ? getColumnStringValue(arguments[3].column).toString() : "") + "',"
                               + std::to_string(max_execute_time_in_second) + ")"
                               + " as result;";
                LOG_TRACE((&Poco::Logger::get("getMapKeys")), "Sending query: {}", query);

                size_t max_query_size = context.getSettings().max_query_size;
                const char * end = query.data() + query.size();
                ParserQuery parser(end/*, context.getSettings().enable_debug_queries*/);
                ASTPtr ast = parseQuery(parser, query, "", max_query_size, 0);
                QueryProcessingStage::Enum processed_stage = QueryProcessingStage::Enum::Complete;
                Block send_block;
                ColumnWithTypeAndName col;
                col.name = "result";
                col.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
                col.column = col.type->createColumn();
                send_block.insert(col);

                ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
                        send_block, processed_stage, QualifiedTableName{remote_db_name, remote_table_name}, {}, false, slowDiag);

                BlockInputStreams inputs = ClusterProxy::executeQuery(select_stream_factory, cluster, ast, context, context.getSettings());
                BlockInputStreamPtr input = std::make_shared<UnionBlockInputStream>(inputs, nullptr,context.getSettings().max_distributed_connections);

                input->readPrefix();
                while (Block current = input->read())
                {
                    Field field;
                    current.getByName("result").column->get(0, field);
                    const Array & tmp_array = DB::get<const Array &>(field);
                    for (auto & keyName: tmp_array)
                    {
                        mapKeySet.insert(keyName.safeGet<String>());
                    }
                }

                Array array;
                for (const String& vec: mapKeySet)
                {
                    array.push_back(Field(vec.data(), vec.size()));
                }

                col_res->insert(array);

                return col_res; 
            }

            const MergeTreeData * merge_tree = dynamic_cast<MergeTreeData *>(table.get());
            if (!merge_tree)
            {
                throw Exception("Not support table.", ErrorCodes::BAD_ARGUMENTS);
            }

            const DataTypeByteMap * map = checkAndGetDataType<DataTypeByteMap>(merge_tree->getColumn(column_name.toString()).type.get());

            if (!map)
                throw Exception("Column must be map.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            UInt64 deadline = max_execute_time_in_second * 1000;
            Stopwatch watch;
            for (auto data_part: merge_tree->getDataParts())
            {
                String partname = data_part->name;
                if (partname.find("_") == std::string::npos)
                    continue;
                if (has_partition_expression)
                {
                    auto& partition_value = data_part->partition.value;
                    FieldVisitorToString to_string_visitor;
                    String compare_result;
                    for (size_t i = 0; i < partition_value.size(); ++i)
                    {
                        if (i > 0)
                            compare_result += '-';

                        if (typeid_cast<const DataTypeDate *>(merge_tree->partition_key_sample.getByPosition(i).type.get()))
                            compare_result += toString(DateLUT::instance().toNumYYYYMMDD(DayNum(partition_value[i].safeGet<UInt64>())));
                        else
                            compare_result += applyVisitor(to_string_visitor, partition_value[i]);
                    }
                    if (!std::regex_match(compare_result, partition_expression))
                        continue;
                }

                {
                    auto data_lock = data_part->getColumnsReadLock();
                    for (auto & file : data_part->checksums.files)
                    {
                        const String & file_name = file.first;
                        String keyName;
                        bool parseSuccess = parseKeyFromImplicitFileName(column_name.toString(), file_name, keyName);
                        if (parseSuccess)
                        {
                            mapKeySet.insert(keyName);
                        }
                    }
                }

                if (watch.elapsedMilliseconds() > deadline)
                {
                    LOG_TRACE((&Poco::Logger::get("getMapKeys")), "getMapKeys time out!");
                    break;
                }
            }

            Array array;
            auto& keyType = map->getKeyType();
            for (const String& vec: mapKeySet)
            {
                Field field = keyType->stringToVisitorField(vec);
                if (isDateOrDateTime(keyType))
                {
                    if (isDate(keyType))
                    {
                        WriteBufferFromOwnString wb;
                        writeDateText(DayNum(field.safeGet<UInt64>()), wb);
                        array.push_back(wb.str());
                    }
                    else
                    {
                        WriteBufferFromOwnString wb;
                        writeDateTimeText(field.safeGet<UInt64>(), wb);
                        array.push_back(wb.str());
                    }
                }
                else if (isStringOrFixedString(keyType))
                    array.push_back(field);
                else
                    array.push_back(applyVisitor(DB::FieldVisitorToString(), field));
            }
            col_res->insert(array);
        }

		return col_res;
#endif
    }

private:
    const ContextPtr context;
};

class FunctionStrToMap: public IFunction
{
public:
    static constexpr auto name = "str_to_map";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionStrToMap>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 3; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    /// throw error when argument[0] column is Nullable(String)
    /// TODO : add Function Convert Nullable(String) column to String column
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception("Function " + getName() + " requires 3 argument. Passed " + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isString(arguments[0].type))
            throw Exception("First argument for function " + getName() + "  must be String, but parsed " + arguments[0].type->getName() + ".",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1].type))
            throw Exception("Second argument for function " + getName() + " (delimiter) must be String.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[2].type))
            throw Exception("Third argument for function " + getName() + " (delimiter) must be String.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeByteMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

	ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnPtr column_prt = arguments[0].column;
        auto item_delimiter = getDelimiter(arguments[1].column);
        auto key_value_delimiter = getDelimiter(arguments[2].column);
        auto col_res = result_type->createColumn();
        ByteMap map;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const StringRef & str = column_prt->getDataAt(i);
            String parsed_value, parsed_key;
            map.clear();
            auto start = str.data, end = str.data + str.size;
            size_t pos = 0, size = str.size;
            while (pos < size)
            {
                parseStringValue(str.data, end, pos, parsed_key, key_value_delimiter);
                while (pos < size && *(start + pos) == ' ') ++pos;
                if(pos == size)
                    throw Exception("Function " + getName() + " parse error, The number of Key and Value not equal.", ErrorCodes::CANNOT_CONVERT_TYPE);
                parseStringValue(str.data, end, pos, parsed_value, item_delimiter);
                while (pos < size && *(start + pos) == ' ') ++pos;
                map.push_back({parsed_key, parsed_value});
            }
            col_res->insert(map);
        }
        return col_res;
    }

private:

    inline void parseStringValue(const char * start, const char * end, size_t & pos, String & res, char delimiter) const
    {
        res = "";
        while (start + pos != end && *(start + pos) != delimiter)
        {
            res += *(start + pos);
            ++pos;
        }
        if (start + pos != end && *(start + pos) == delimiter) ++pos;
    }

    inline char getDelimiter(const ColumnPtr & column_ptr) const
    {
        // TODO: recheck column type
        const auto & value = dynamic_cast<const ColumnUInt8*>(column_ptr.get())->getDataAt(0);
        return value.data[0];
    }
};

class FunctionMapConstructor: public IFunction
{
public:
    static constexpr auto name = "map";

    static FunctionPtr create(const ContextPtr &) { return std::make_shared<FunctionMapConstructor>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return false; }

    /// pasr map(k1, v1, k2, v2 ...)
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if ((arguments.size() & 1) || arguments.empty())
            throw Exception("Function " + getName() + " requires none zero even number of argument. Passed " + toString(arguments.size()) + ".",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes key_types;
        DataTypes value_types;
        key_types.reserve(arguments.size() >> 1);
        value_types.reserve(arguments.size() >> 1);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (i & 1)
                value_types.push_back(arguments[i].type);
            else
                key_types.push_back(arguments[i].type);
        }

        auto key_type = getLeastSupertype(key_types);
        auto value_type = getLeastSupertype(value_types);

        if (key_type->isNullable() || value_type->isNullable())
            throw Exception("KeyType and ValueType for Map should be non-nullable.", ErrorCodes::TYPE_MISMATCH);

        return std::make_shared<DataTypeByteMap>(key_type, value_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto col_res = result_type->createColumn();

        ByteMap map;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            map.clear();
            for(size_t pos = 0; pos < arguments.size(); pos += 2)
            {
                auto key = (*arguments[pos].column)[i];
                auto value = (*arguments[pos + 1].column)[i];
                map.push_back({key, value});
            }
            col_res->insert(map);
        }
        return col_res;
    }
        
};

struct NameExtractMapColumn
{
    static constexpr auto name = "extractMapColumn";
};

struct NameExtractMapKey
{
    static constexpr auto name = "extractMapKey";
};

template <class Extract>
struct ExtractMapWrapper
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        size_t offsets_size = offsets.size();
        res_offsets.resize(offsets_size);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets_size; ++i)
        {
            const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
            size_t src_size = offsets[i] - prev_offset;
            auto res_view = Extract::apply(std::string_view(src_data, src_size));
            memcpy(reinterpret_cast<char *>(res_data.data() + res_offset), res_view.data(), res_view.size());

            res_offset += res_view.size() + 1; /// remember add 1 for null char
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }

        res_data.resize(res_offset);
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Column of type FixedString is not supported by extractMapColumn", ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionsByteMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapElement>();
    factory.registerFunction<FunctionMapKeys>();
    factory.registerFunction<FunctionMapValues>();
    factory.registerFunction<FunctionGetMapKeys>();
    factory.registerFunction<FunctionStrToMap>();
    factory.registerFunction<FunctionMapConstructor>();

    using FunctionExtractMapColumn = FunctionStringToString<ExtractMapWrapper<ExtractMapColumn>, NameExtractMapColumn>;
    using FunctionExtractMapKey = FunctionStringToString<ExtractMapWrapper<ExtractMapKey>, NameExtractMapKey>;
    factory.registerFunction<FunctionExtractMapKey>();
    factory.registerFunction<FunctionExtractMapColumn>();
}

}
