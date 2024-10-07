
#pragma once

#include <string>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeJsonb.h>
#include <Common/JSONParsers/jsonb/JSONBDocument.h>
#include <Common/JSONParsers/jsonb/JSONBUtils.h>
#include <Common/JSONParsers/jsonb/JSONBWriter.h>
#include <common/defines.h>
#include "Columns/ColumnJsonb.h"
#include <DataTypes/Serializations/JSONBValue.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

class FunctionJSONBuildArray : public IFunction
{
public:
    constexpr static auto name = "json_build_array";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONBuildArray>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        MutableColumnPtr result{result_type->createColumn()};
        result->reserve(input_rows_count);

        auto *to = assert_cast<ColumnString *>(result.get());

        auto & to_chars = to->getChars();
        auto & to_offsets = to->getOffsets();

        to_chars.resize(input_rows_count);
        to_offsets.resize(input_rows_count);

        FormatSettings format_settings;
        WriteBufferFromVector buf(to_chars);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            buf.write('[');
            for (size_t j = 0; j < arguments.size(); ++j)
            {
                const auto & col_from = *arguments[j].column;
                const auto type = arguments[j].type;
                if (isString(type))
                {
                    buf.write('"');
                    type->getDefaultSerialization()->serializeText(col_from, i, buf, format_settings);
                    buf.write('"');
                }
                else
                    type->getDefaultSerialization()->serializeText(col_from, i, buf, format_settings);

                if (j < arguments.size() - 1)
                    buf.write(',');
            }
            buf.write(']');
            writeChar(0, buf);
            to_offsets[i] = buf.count();
        }

        buf.finalize();
        return result;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }
};

class FunctionJSONObject : public IFunction
{
public:
    constexpr static auto name = "json_object";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONObject>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        MutableColumnPtr result{result_type->createColumn()};
        result->reserve(input_rows_count);

        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 1 or 2 arguments", getName());
        
        auto *to = assert_cast<ColumnString *>(result.get());

        auto & to_chars = to->getChars();
        auto & to_offsets = to->getOffsets();

        to_chars.resize(input_rows_count);
        to_offsets.resize(input_rows_count);

        WriteBufferFromVector buf(to_chars);

        if (arguments.size() == 1)
        {
            const auto & first_arg = arguments[0];
            if (!isString(first_arg.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be String", getName());

            const auto * col_from = assert_cast<const ColumnString *>(first_arg.column.get());
            const auto & from_chars = col_from->getChars();
            const auto & from_offsets = col_from->getOffsets();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string kvs{reinterpret_cast<const char *>(&from_chars[from_offsets[i - 1]]), from_offsets[i] - from_offsets[i - 1] - 1};
                boost::trim(kvs);
                if (kvs.starts_with('{') && kvs.ends_with('}') && kvs.size() % 2 == 0)
                {
                    auto real_kvs = kvs.substr(1, kvs.size() - 2);
                    std::vector<std::string> real_kvs_array;
                    boost::split(real_kvs_array, real_kvs, boost::is_any_of(","));
                    
                    buf.write('{');
                    for (size_t j = 0; j + 1 < real_kvs_array.size(); j+=2)
                    {
                        auto key = real_kvs_array[j];
                        auto value = real_kvs_array[j + 1];
                        boost::trim(key);
                        boost::trim(value);
                        boost::trim_if(key, boost::is_any_of("{"));
                        boost::trim_if(key, boost::is_any_of("}"));
                        boost::trim_if(value, boost::is_any_of("{"));
                        boost::trim_if(value, boost::is_any_of("}"));
                        boost::trim_if(key, boost::is_any_of("\""));
                        boost::trim_if(value, boost::is_any_of("\""));
                        writeDoubleQuoted(key, buf);
                        buf.write(':');
                        writeDoubleQuoted(value, buf);

                        if (j < real_kvs_array.size() - 2)
                            buf.write(',');
                    }
                    buf.write('}');
                    writeChar(0, buf);
                }
                else
                {
                    buf.write('{');
                    buf.write('}');
                    writeChar(0, buf);
                }

                to_offsets[i] = buf.count();
            }
        }
        else
        {
            const auto & keys_arg = arguments[0];
            const auto & values_arg = arguments[1];
            if (!isString(keys_arg.type) && !isString(values_arg.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Two argument for function {} must be String", getName());
            const auto * keys_column = assert_cast<const ColumnString *>(keys_arg.column.get());
            const auto & keys_chars = keys_column->getChars();
            const auto & keys_offsets = keys_column->getOffsets();

            const auto * values_column = assert_cast<const ColumnString *>(values_arg.column.get());
            const auto & values_chars = values_column->getChars();
            const auto & values_offsets = values_column->getOffsets();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string keys{reinterpret_cast<const char *>(&keys_chars[keys_offsets[i - 1]]), keys_offsets[i] - keys_offsets[i - 1] - 1};
                std::string values{reinterpret_cast<const char *>(&values_chars[values_offsets[i - 1]]), values_offsets[i] - values_offsets[i - 1] - 1};
                boost::trim(keys);
                boost::trim(values);
                if (keys.starts_with('{') && keys.ends_with('}') && values.starts_with('{') && values.ends_with('}'))
                {
                    auto real_keys = keys.substr(1, keys.size() - 2);
                    std::vector<std::string> real_keys_array;
                    boost::split(real_keys_array, real_keys, boost::is_any_of(","));

                    auto real_values = values.substr(1, values.size() - 2);
                    std::vector<std::string> real_values_array;
                    boost::split(real_values_array, real_values, boost::is_any_of(","));

                    if (real_keys_array.size() != real_values_array.size())
                    {
                        buf.write('{');
                        buf.write('}');
                        writeChar(0, buf);
                        continue;
                    }

                    buf.write('{');
                    for (size_t j = 0; j < real_keys_array.size(); ++j)
                    {
                        auto key = real_keys_array[j];
                        auto value = real_values_array[j];
                        boost::trim(key);
                        boost::trim(value);
                        boost::trim_if(key, boost::is_any_of("\""));
                        boost::trim_if(value, boost::is_any_of("\""));
                        writeDoubleQuoted(key, buf);
                        buf.write(':');
                        writeDoubleQuoted(value, buf);

                        if (j < real_keys_array.size() - 1)
                            buf.write(',');
                    }
                    buf.write('}');
                    writeChar(0, buf);
                }
                else
                {
                    buf.write('{');
                    buf.write('}');
                    writeChar(0, buf);
                }

                to_offsets[i] = buf.count();   
            }
        }

        buf.finalize();

        return result;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }
};

class FunctionJSONBExtract : public IFunction
{
public:
    constexpr static auto name = "jsonb_extract";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONBExtract>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        MutableColumnPtr result{result_type->createColumn()};
        result->reserve(input_rows_count);

        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 1 or 2 arguments", getName());

        auto * to = assert_cast<ColumnJsonb *>(result.get());

        auto & to_chars = to->getChars();
        auto & to_offsets = to->getOffsets();

        to_chars.resize(input_rows_count);
        to_offsets.resize(input_rows_count);

        WriteBufferFromVector buf(to_chars);

        const auto & first_arg = arguments[0];
        if (!isJsonb(first_arg.type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be JSONB", getName());

        const auto * jsonb_col_const = typeid_cast<const ColumnConst *>(first_arg.column.get());

        const auto * jsonb_col
            = typeid_cast<const ColumnJsonb *>(jsonb_col_const ? jsonb_col_const->getDataColumnPtr().get() : first_arg.column.get());
        const auto & second_arg = arguments[1];
        if (!isString(second_arg.type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be String", getName());

        const auto * jsonb_path_const = typeid_cast<const ColumnConst *>(second_arg.column.get());
        const auto * jsonb_path_string
            = typeid_cast<const ColumnString *>(jsonb_path_const ? jsonb_path_const->getDataColumnPtr().get() : second_arg.column.get());
        JsonbPath jsonb_path;
        JsonbWriter writer;
        if (jsonb_path_const)
        {
            if (!jsonb_path.seek(jsonb_path_string->getDataAt(0).data, jsonb_path_string->getDataAt(0).size))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal JSONB path: {}", jsonb_path_string->getDataAt(0).toString());
            }
        }

        auto write_default_jsonb = [&buf]() {
            JsonBinaryValue jsonb_default;
            jsonb_default.fromJsonString("{}", 2);
            buf.write(jsonb_default.value(), jsonb_default.size());
        };

        for (size_t i = 0; i < input_rows_count; i++)
        {
            if (!jsonb_path_const)
            {
                if (!jsonb_path.seek(jsonb_path_string->getDataAt(i).data, jsonb_path_string->getDataAt(i).size))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal JSONB path: {}", jsonb_path_string->getDataAt(i).toString());
                }
            }

            auto binary_json_strref = jsonb_col->getDataAt(i);
            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument * doc = JsonbDocument::createDocument(binary_json_strref.data, binary_json_strref.size);
            if (unlikely(!doc || !doc->getValue()))
            {
                write_default_jsonb();
            }
            else
            {
                // value is NOT necessary to be deleted since JsonbValue will not allocate memory
                JsonbValue * value = doc->getValue()->findValue(jsonb_path, nullptr);
                if (value)
                {
                    writer.reset();
                    writer.writeValue(value);
                    buf.write(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
                }
                else
                {
                    write_default_jsonb();
                }
            }

            writeChar(0, buf);
            to_offsets[i] = buf.count();   
        }

        buf.finalize();

        return result;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeJsonb>();
    }
};

} // namespace DB
