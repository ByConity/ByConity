#pragma once

#include <sstream>
#include <string>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/ObjectUtils.h>
#include <Functions/FunctionsJSON.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Generator/ObjectJSONGeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Interpreters/Context.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <magic_enum.hpp>
#include <Common/assert_cast.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <common/range.h>
#include <Core/SettingsEnums.h>
#include <Columns/ColumnObject.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionsComparison.h>
#include <Parsers/IAST_fwd.h>
#include <Functions/FunctionFactory.h>
#include <bits/stdint-uintn.h>

#if !defined(ARCADIA_BUILD)
#include "config_functions.h"
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

template <typename JSONParser>
class JSONUtils
{
public:
    using Element = typename JSONParser::Element;
    using Object = typename JSONParser::Object;
    using Array = typename JSONParser::Array;

    static bool jsonElementEqual(const Element & left, const Element & right)
    {
        if (left.isInt64() && right.isInt64())
        {
            return left.getInt64() == right.getInt64();
        }
        else if (left.isUInt64() && right.isUInt64())
        {
            return left.getUInt64() == right.getUInt64();
        }
        else if (left.isDouble() && right.isDouble())
        {
            return left.getDouble() == right.getDouble();
        }
        else if (left.isString() && right.isString())
        {
            return left.getString() == right.getString();
        }
        else if (left.isBool() && right.isBool())
        {
            return left.getBool() == right.getBool();
        }
        else if (left.isNull() && right.isNull())
        {
            return true;
        }

        return false;
    }

    static bool jsonArrayContains(const Array & json_array, const Element & sub_element)
    {
        if (sub_element.isArray())
        {
            const auto & sub_array = sub_element.getArray();
            for (auto it = sub_array.begin(); it != sub_array.end(); ++it)
            {
                if (!jsonArrayContains(json_array, *it))
                {
                    return false;
                }
            }
        }
        else if (sub_element.isObject())
        {
            return false;
        }
        else
        {
            for (auto it = json_array.begin(); it != json_array.end(); ++it)
            {
                if (jsonElementEqual(*it, sub_element))
                {
                    return true;
                }
            }

            return false;
        }

        return true;
    }

    static bool jsonObjectContains(const Object & json_object, const Element & sub_element)
    {
        if (sub_element.isObject())
        {
            for (const auto & [key, value] : sub_element.getObject())
            {
                Element temp_element;
                bool contains_key = json_object.find(key, temp_element);
                if (!contains_key)
                    return false;

                if (temp_element.isObject())
                {
                    if (!jsonObjectContains(temp_element.getObject(), value))
                        return false;
                    else
                        continue;
                }

                if (temp_element.isArray())
                {
                    if (!jsonArrayContains(temp_element.getArray(), value))
                        return false;
                    else
                        continue;
                }

                if (!jsonElementEqual(temp_element, value))
                    return false;
            }
        }
        else
        {
            return false;
        }

        return true;
    }

    static bool contains(const Element & parent_element, const Element & sub_element)
    {
        if (parent_element.isObject())
        {
            return jsonObjectContains(parent_element.getObject(), sub_element);
        }
        else if (parent_element.isArray())
        {
            return jsonArrayContains(parent_element.getArray(), sub_element);
        }
        else
        {
            return jsonElementEqual(parent_element, sub_element);
        }
    }
};

class FunctionSQLJSONHelpers
{
public:
    template <typename Name, template <typename> typename Impl, class JSONParser>
    class ExecutorString
    {
    public:
        static ColumnPtr
        run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth, DialectType dialect_type)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            const auto & json_column = arguments[0];

            if (!isString(json_column.type))
            {
                throw Exception(
                    "JSONPath functions require first argument to be JSON of string, illegal type: " + json_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const auto & json_path_column = arguments[1];

            if (!isString(json_path_column.type))
            {
                throw Exception(
                    "JSONPath functions require second argument to be JSONPath of type string, illegal type: "
                        + json_path_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            if (!isColumnConst(*json_path_column.column))
            {
                throw Exception("Second argument (JSONPath) must be constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const ColumnPtr & arg_jsonpath = json_path_column.column;
            const auto * arg_jsonpath_const = typeid_cast<const ColumnConst *>(arg_jsonpath.get());
            const auto * arg_jsonpath_string = typeid_cast<const ColumnString *>(arg_jsonpath_const->getDataColumnPtr().get());

            const ColumnPtr & arg_json = json_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            /// Get data and offsets for 1 argument (JSONPath)
            const ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Prepare to parse 1 argument (JSONPath)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                if (dialect_type != DialectType::MYSQL)
                    throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
                else
                {
                    to->insertManyDefaults(input_rows_count);
                    return to;
                }
            }

            /// Get data and offsets for 2 argument (JSON)
            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl<ElementIterator<JSONParser>> impl;

            constexpr bool has_member_prepare = requires
            {
                impl.prepare("", DataTypePtr{});
            };

            if constexpr (has_member_prepare)
                impl.prepare(Name::name, result_type);

            for (const auto i : collections::range(0, input_rows_count))
            {
                std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                ElementIterator<JSONParser> iterator(document);
                if (document_ok)
                {
                    added_to_column = impl.insertResultToColumn(*to, iterator, res, dialect_type);
                }
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
            return to;
        }
    };


    template <typename Name, template <typename> typename Impl, class JSONParser>
    class ExecutorObject
    {
    public:
        template <typename ArgColumn>
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth, DialectType dialect_type)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            const auto & json_column = arguments[0];

            if (!isObject(json_column.type) && !isTuple(json_column.type))
            {
                throw Exception(
                    "JSONPath functions require first argument to be JSON of Object or Tuple, illegal type: " + json_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const auto & json_path_column = arguments[1];

            if (!isString(json_path_column.type))
            {
                throw Exception(
                    "JSONPath functions require second argument to be JSONPath of type string, illegal type: "
                        + json_path_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            if (!isColumnConst(*json_path_column.column))
            {
                throw Exception("Second argument (JSONPath) must be constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const ColumnPtr & arg_jsonpath = json_path_column.column;
            const auto * arg_jsonpath_const = typeid_cast<const ColumnConst *>(arg_jsonpath.get());
            const auto * arg_jsonpath_string = typeid_cast<const ColumnString *>(arg_jsonpath_const->getDataColumnPtr().get());

            const ColumnPtr & arg_json = json_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_object
                = typeid_cast<const ArgColumn *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            ColumnPtr column_tuple;
            DataTypePtr type_tuple;

            if constexpr (std::is_same_v<ArgColumn, ColumnObject>)
            {
                std::tie(column_tuple, type_tuple) = unflattenObjectToTuple(*col_json_object);
            }
            else
            {
                column_tuple = col_json_object->getPtr();
                type_tuple = json_column.type;
            }

            /// Get data and offsets for 1 argument (JSONPath)
            const ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Prepare to parse 1 argument (JSONPath)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                if (dialect_type != DialectType::MYSQL)
                    throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
                else
                {
                    to->insertManyDefaults(input_rows_count);
                    return to;
                }
            }

            // Element document;

            /// Parse JSON for every row
            Impl<ObjectIterator> impl;

            constexpr bool has_member_prepare = requires
            {
                impl.prepare("", DataTypePtr{});
            };

            if constexpr (has_member_prepare)
                impl.prepare(Name::name, result_type);

            for (const auto i : collections::range(0, input_rows_count))
            {
                ObjectIterator iterator(type_tuple, column_tuple, col_json_const ? 0 : i);
                bool added_to_column = impl.insertResultToColumn(*to, iterator, res, dialect_type);

                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
            return to;
        }
    };
};

template <typename Name, typename Derived>
class ExecutableFunctionSQLJSONBase : public IExecutableFunction
{

public:
    explicit ExecutableFunctionSQLJSONBase(const NullPresence & null_presence_, const DataTypePtr & json_return_type_, uint32_t parser_depth_, DialectType dialect_type_)
        : null_presence(null_presence_), json_return_type(json_return_type_), parser_depth(parser_depth_), dialect_type(dialect_type_)
    {
    }

    String getName() const override { return Name::name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        auto temp_arguments = null_presence.has_nullable ? createBlockWithNestedColumns(arguments) : arguments;
        auto temporary_result = Derived::run(temp_arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
        if (null_presence.has_nullable)
            return wrapInNullable(temporary_result, arguments, result_type, input_rows_count);
        return temporary_result;
    }

private:
    NullPresence null_presence;
    DataTypePtr json_return_type;
    uint32_t parser_depth;
    DialectType dialect_type;
};

template <typename Name, template<typename> typename Impl, bool allow_simdjson>
class ExecutableFunctionSQLJSONString : public ExecutableFunctionSQLJSONBase<Name, ExecutableFunctionSQLJSONString<Name, Impl, allow_simdjson>>
{
public:
    using Base = ExecutableFunctionSQLJSONBase<Name, ExecutableFunctionSQLJSONString>;

    ExecutableFunctionSQLJSONString(const NullPresence & null_presence_, const DataTypePtr & json_return_type_, uint32_t parser_depth_, DialectType dialect_type_)
        : Base(null_presence_, json_return_type_, parser_depth_, dialect_type_)
    {
    }

    static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & json_return_type, size_t input_rows_count, uint32_t parser_depth, const DialectType & dialect_type)
    {
        auto temp_arguments = arguments;
        if (temp_arguments.size() < 2)
        {
            DataTypePtr default_path_type = std::make_shared<DataTypeString>();
            MutableColumnPtr default_path_string_column = default_path_type->createColumn();
            default_path_string_column->insert("$");
            MutableColumnPtr default_path_column = ColumnConst::create(std::move(default_path_string_column), 1);
            temp_arguments.emplace_back(ColumnWithTypeAndName(std::move(default_path_column), default_path_type, "$"));
        }

        return chooseAndRunJSONParser(temp_arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
    }

private:
    static ColumnPtr chooseAndRunJSONParser(const ColumnsWithTypeAndName & arguments, const DataTypePtr & json_return_type, size_t input_rows_count, uint32_t parser_depth, const DialectType & dialect_type)
    {
#if USE_SIMDJSON
        if constexpr (allow_simdjson)
            return FunctionSQLJSONHelpers::ExecutorString<Name, Impl, SimdJSONParser>::run(arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
#endif

        return FunctionSQLJSONHelpers::ExecutorString<Name, Impl, DummyJSONParser>::run(arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
    }
};

template <typename Name, template<typename> typename Impl, bool allow_simdjson>
class ExecutableFunctionSQLJSONObject : public ExecutableFunctionSQLJSONBase<Name, ExecutableFunctionSQLJSONObject<Name, Impl, allow_simdjson>>
{
public:
    using Base = ExecutableFunctionSQLJSONBase<Name, ExecutableFunctionSQLJSONObject>;

    ExecutableFunctionSQLJSONObject(const NullPresence & null_presence_, const DataTypePtr & json_return_type_, uint32_t parser_depth_, DialectType dialect_type_)
        : Base(null_presence_, json_return_type_, parser_depth_, dialect_type_)
    {
    }

    static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & json_return_type, size_t input_rows_count, uint32_t parser_depth, DialectType dialect_type)
    {
        assert(!arguments.empty());

        auto temp_arguments = arguments;
        if (temp_arguments.size() < 2)
        {
            DataTypePtr default_path_type = std::make_shared<DataTypeString>();
            MutableColumnPtr default_path_string_column = default_path_type->createColumn();
            default_path_string_column->insert("$");
            MutableColumnPtr default_path_column = ColumnConst::create(std::move(default_path_string_column), 1);
            temp_arguments.emplace_back(ColumnWithTypeAndName(std::move(default_path_column), default_path_type, "$"));
        }
        const auto & type_object = assert_cast<const DataTypeObject &>(*temp_arguments[0].type);
        const auto & arg_object = temp_arguments[0].column;
        const auto * column_const = typeid_cast<const ColumnConst *>(arg_object.get());
        const auto * column_object
            = typeid_cast<const ColumnObject *>(column_const ? column_const->getDataColumnPtr().get() : arg_object.get());

        assert(column_object);
        if (column_object->hasNullableSubcolumns())
        {
            auto non_nullable_object = ColumnObject::create(false);
            for (const auto & entry : column_object->getSubcolumns())
            {
                auto new_subcolumn = recursiveAssumeNotNullable(entry->data.getFinalizedColumnPtr());
                non_nullable_object->addSubcolumn(entry->path, new_subcolumn->assumeMutable());
            }

            temp_arguments[0].type = std::make_shared<DataTypeObject>(type_object.getSchemaFormat(), false);
            temp_arguments[0].column = std::move(non_nullable_object);

            if (column_const)
                temp_arguments[0].column = ColumnConst::create(temp_arguments[0].column, column_const->size());
        }

#if USE_SIMDJSON
        if constexpr (allow_simdjson)
        {
            return FunctionSQLJSONHelpers::ExecutorObject<Name, Impl, SimdJSONParser>::template run<ColumnObject>(
                temp_arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
        }
#endif

        return FunctionSQLJSONHelpers::ExecutorObject<Name, Impl, DummyJSONParser>::template run<ColumnObject>(
            temp_arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
    }
};

template <typename Name, template<typename> typename Impl, bool allow_simdjson>
class ExecutableFunctionSQLJSONTuple : public ExecutableFunctionSQLJSONBase<Name, ExecutableFunctionSQLJSONTuple<Name, Impl, allow_simdjson>>
{
public:
    using Base = ExecutableFunctionSQLJSONBase<Name, ExecutableFunctionSQLJSONTuple>;

    ExecutableFunctionSQLJSONTuple(const NullPresence & null_presence_, const DataTypePtr & json_return_type_, uint32_t parser_depth_, DialectType dialect_type_)
        : Base(null_presence_, json_return_type_, parser_depth_, dialect_type_)
    {
    }

    static ColumnPtr
    run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & json_return_type, size_t input_rows_count, uint32_t parser_depth, DialectType dialect_type)
    {
        auto temp_arguments = arguments;
        if (temp_arguments.size() < 2)
        {
            DataTypePtr default_path_type = std::make_shared<DataTypeString>();
            MutableColumnPtr default_path_string_column = default_path_type->createColumn();
            default_path_string_column->insert("$");
            MutableColumnPtr default_path_column = ColumnConst::create(std::move(default_path_string_column), 1);
            temp_arguments.emplace_back(ColumnWithTypeAndName(std::move(default_path_column), default_path_type, "$"));
        }
#if USE_SIMDJSON
        if constexpr (allow_simdjson)
        {
            return FunctionSQLJSONHelpers::ExecutorObject<Name, Impl, SimdJSONParser>::template run<ColumnTuple>(
                temp_arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
        }
#endif

        return FunctionSQLJSONHelpers::ExecutorObject<Name, Impl, DummyJSONParser>::template run<ColumnTuple>(
            temp_arguments, json_return_type, input_rows_count, parser_depth, dialect_type);
    }
};


template <typename Name>
class FunctionBaseFunctionSQLJSON : public IFunctionBase
{
public:
    String getName() const override { return Name::name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return return_type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

protected:
    explicit FunctionBaseFunctionSQLJSON(
        const NullPresence & null_presence_,
        DataTypes argument_types_,
        DataTypePtr return_type_,
        DataTypePtr json_return_type_,
        uint32_t parser_depth_,
        DialectType dialect_type_)
        : null_presence(null_presence_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
        , json_return_type(std::move(json_return_type_))
        , parser_depth(parser_depth_)
        , dialect_type(dialect_type_)
    {
    }

    NullPresence null_presence;
    bool allow_simdjson;
    DataTypes argument_types;
    DataTypePtr return_type;
    DataTypePtr json_return_type;
    uint32_t parser_depth;
    DialectType dialect_type;
};

template <typename Name, template<typename> typename Impl>
class FunctionBaseFunctionSQLJSONString : public FunctionBaseFunctionSQLJSON<Name>
{
public:
    template <typename... Args>
    explicit FunctionBaseFunctionSQLJSONString(bool allow_simdjson_, Args &&... args)
        : FunctionBaseFunctionSQLJSON<Name>{std::forward<Args>(args)...}
        , allow_simdjson(allow_simdjson_)
    {
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        if (this->allow_simdjson)
            return std::make_unique<ExecutableFunctionSQLJSONString<Name, Impl, true>>(this->null_presence, this->json_return_type, this->parser_depth, this->dialect_type);

        return std::make_unique<ExecutableFunctionSQLJSONString<Name, Impl, false>>(this->null_presence, this->json_return_type, this->parser_depth, this->dialect_type);
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
private:
    bool allow_simdjson;
};

template <typename Name, template<typename> typename Impl>
class FunctionBaseFunctionSQLJSONObject : public FunctionBaseFunctionSQLJSON<Name>
{
public:
    template <typename... Args>
    explicit FunctionBaseFunctionSQLJSONObject(bool allow_simdjson_, Args &&... args)
        : FunctionBaseFunctionSQLJSON<Name>{std::forward<Args>(args)...}
        , allow_simdjson(allow_simdjson_)
    {
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        if (this->allow_simdjson)
            return std::make_unique<ExecutableFunctionSQLJSONObject<Name, Impl, true>>(this->null_presence, this->json_return_type, this->parser_depth, this->dialect_type);

        return std::make_unique<ExecutableFunctionSQLJSONObject<Name, Impl, false>>(this->null_presence, this->json_return_type, this->parser_depth, this->dialect_type);
    }

private:
    bool allow_simdjson;
};

template <typename Name, template<typename> typename Impl>
class FunctionBaseFunctionSQLJSONTuple : public FunctionBaseFunctionSQLJSON<Name>
{
public:
    template <typename... Args>
    explicit FunctionBaseFunctionSQLJSONTuple(bool allow_simdjson_, Args &&... args)
        : FunctionBaseFunctionSQLJSON<Name>{std::forward<Args>(args)...}
        , allow_simdjson(allow_simdjson_)
    {
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        if (this->allow_simdjson)
            return std::make_unique<ExecutableFunctionSQLJSONTuple<Name, Impl, true>>(this->null_presence, this->json_return_type, this->parser_depth, this->dialect_type);

        return std::make_unique<ExecutableFunctionSQLJSONTuple<Name, Impl, false>>(this->null_presence, this->json_return_type, this->parser_depth, this->dialect_type);
    }
private:
    bool allow_simdjson;
};

using ObjectIterator = FunctionJSONHelpers::ObjectIterator;
template<typename JSONParser>
using ElementIterator = FunctionJSONHelpers::JSONElementIterator<JSONParser>;

/// We use IFunctionOverloadResolver instead of IFunction to handle non-default NULL processing.
/// Both NULL and JSON NULL should generate NULL value. If any argument is NULL, return NULL.
template <typename Name, template<typename> typename Impl>
class SQLJSONOverloadResolver : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = Name::name;

    String getName() const override { return name; }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<SQLJSONOverloadResolver>(context_);
    }

    explicit SQLJSONOverloadResolver(ContextPtr context_) : WithContext(context_) {}

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const override
    {
        bool has_nothing_argument = false;
        for (const auto & arg : arguments)
            has_nothing_argument |= isNothing(arg.type);

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one argument", Name::name);

        const auto & first_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(first_column.type));

        bool is_string = isString(first_type_base);
        bool is_object = isObject(first_type_base);
        bool is_tuple = isTuple(first_type_base);
        bool is_nothing = isNothing(first_type_base);

        if (!is_string && !is_object && !is_tuple && !is_nothing)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing JSON or Object or Tuple, illegal type: {}",
                Name::name, first_column.type->getName());

        auto json_return_type = Impl<ObjectIterator>::getReturnType(Name::name, createBlockWithNestedColumns(arguments));
        NullPresence null_presence = getNullPresense(arguments);
        DataTypePtr return_type;
        if (has_nothing_argument)
            return_type = std::make_shared<DataTypeNothing>();
        else if (null_presence.has_null_constant)
            return_type = makeNullable(std::make_shared<DataTypeNothing>());
        else if (null_presence.has_nullable)
            return_type = makeNullable(json_return_type);
        else
            return_type = json_return_type;

        /// Top-level LowCardinality columns are processed outside JSON parser.
        json_return_type = removeLowCardinality(json_return_type);

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.emplace_back(argument.type);

        auto allow_simdjson = getContext()->getSettingsRef().allow_simdjson;
        uint32_t parser_depth = getContext()->getSettingsRef().max_parser_depth;
        DialectType dialect_type = getContext()->getSettingsRef().dialect_type;
        if (is_string || is_nothing)
            return std::make_unique<FunctionBaseFunctionSQLJSONString<Name, Impl>>(
                allow_simdjson, null_presence, argument_types, return_type, json_return_type, parser_depth, dialect_type);
        else if (is_object)
            return std::make_unique<FunctionBaseFunctionSQLJSONObject<Name, Impl>>(
                allow_simdjson, null_presence, argument_types, return_type, json_return_type, parser_depth, dialect_type);
        else
            return std::make_unique<FunctionBaseFunctionSQLJSONTuple<Name, Impl>>(
                allow_simdjson, null_presence, argument_types, return_type, json_return_type, parser_depth, dialect_type);
    }
};

struct NameSQLJSONExists
{
    static constexpr auto name{"JSON_EXISTS"};
};

struct NameSQLJSONValue
{
    static constexpr auto name{"JSON_VALUE"};
};

struct NameSQLJSONQuery
{
    static constexpr auto name{"JSON_QUERY"};
};

struct NameSQLJSONLength
{
    static constexpr auto name{"JSON_LENGTH"};
};

struct NameSQLJSONContains
{
    static constexpr auto name{"JSON_CONTAINS"};
};

struct NameSQLJSONContainsPath
{
    static constexpr auto name{"JSON_CONTAINS_PATH"};
};

struct NameSQLJSONArrayContains
{
    static constexpr auto name{"JSON_ARRAY_CONTAINS"};
};

struct NameSQLJSONKeys
{
    static constexpr auto name{"JSON_KEYS"};
};

struct NameSQLJSONExtract
{
    static constexpr auto name{"JSON_EXTRACT"};
};

template <typename Iterator>
class SQLJSONKeysImpl
{
public:

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_unique<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, Iterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsElementIterator<Iterator>
    {
        using Element = typename Iterator::Element;
        using JSONParser = typename Iterator::JSONParserType;
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = iterator.getElement();
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = iterator.getElement();
        }

        Iterator sub_iterator{current_element};
        return JSONExtractKeysImpl<Iterator>::insertResultToColumn(dest, sub_iterator);
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsObjectIterator<Iterator>
    {
        ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
        }

        return JSONExtractKeysImpl<Iterator>::insertResultToColumn(dest, iterator);
    }
};

template <typename Iterator>
class SQLJSONExtractImpl
{
public:

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, Iterator & iterator, ASTPtr & query_ptr, DialectType dialect_type) requires IsElementIterator<Iterator>
    {
        using Element = typename Iterator::Element;
        using JSONParser = typename Iterator::JSONParserType;
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = iterator.getElement();
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = iterator.getElement();
        }

        if (status == VisitorStatus::Exhausted)
        {
            return false;
        }

        Iterator sub_iterator{current_element};
        return JSONExtractRawImpl<Iterator>::insertResultToColumn(dest, sub_iterator, dialect_type);
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator, ASTPtr & query_ptr, DialectType dialect_type) requires IsObjectIterator<Iterator>
    {
        ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
        }

        if (status == VisitorStatus::Exhausted)
        {
            return false;
        }

        return JSONExtractRawImpl<Iterator>::insertResultToColumn(dest, iterator, dialect_type);
    }
};

template <typename Iterator>
class SQLJSONExistsImpl
{
public:

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, Iterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsElementIterator<Iterator>
    {
        using Element = typename Iterator::Element;
        using JSONParser = typename Iterator::JSONParserType;
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = iterator.getElement();
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = iterator.getElement();
        }

        /// insert result, status can be either Ok (if we found the item)
        /// or Exhausted (if we never found the item)
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);
        if (status == VisitorStatus::Ok)
        {
            col_bool.insert(1);
        }
        else
        {
            col_bool.insert(0);
        }
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsObjectIterator<Iterator>
    {
        ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
        }

        /// insert result, status can be either Ok (if we found the item)
        /// or Exhausted (if we never found the item)
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);
        if (status == VisitorStatus::Ok)
        {
            col_bool.insert(1);
        }
        else
        {
            col_bool.insert(0);
        }
        return true;
    }
};

template <typename Iterator>
class SQLJSONValueImpl
{
public:

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, Iterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsElementIterator<Iterator>
    {
        using Element = typename Iterator::Element;
        using JSONParser = typename Iterator::JSONParserType;
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = iterator.getElement();
        VisitorStatus status;
        Element res;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                if (!(current_element.isArray() || current_element.isObject()))
                {
                    break;
                }
            }
            else if (status == VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = iterator.getElement();
        }

        if (status == VisitorStatus::Exhausted)
        {
            return false;
        }

        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        out << current_element.getElement();
        auto output_str = out.str();
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(output_str.data(), output_str.size());
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsObjectIterator<Iterator>
    {
        ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                const auto & element_type = iterator.getType();
                if (!(isArray(element_type) || isObject(element_type) || isTuple(element_type)))
                    break;
            }
        }

        if (status == VisitorStatus::Exhausted)
        {
            return false;
        }

        auto row = iterator.getRow();
        if (const auto * column_string = typeid_cast<const ColumnString *>(iterator.getColumn().get()))
        {
            dest.insertFrom(*column_string, row);
            return true;
        }

        return JSONExtractRawImpl<Iterator>::insertResultToColumn(dest, iterator);
    }
};

/**
 * Function to test jsonpath member access, will be removed in final PR
 * JSONParser parser
 */
template <typename Iterator>
class SQLJSONQueryImpl
{
public:

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, Iterator & iterator, ASTPtr & query_ptr, DialectType /*dialect_type*/) requires IsElementIterator<Iterator>
    {
        using Element = typename Iterator::Element;
        using JSONParser = typename Iterator::JSONParserType;
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = iterator.getElement();
        VisitorStatus status;
        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        /// Create json array of results: [res1, res2, ...]
        out << "[";
        bool success = false;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                if (success)
                {
                    out << ", ";
                }
                success = true;
                out << current_element.getElement();
            }
            else if (status == VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = iterator.getElement();
        }
        out << "]";
        if (!success)
        {
            return false;
        }
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        auto output_str = out.str();
        col_str.insertData(output_str.data(), output_str.size());
        return true;
    }

    static bool insertResultToColumn(IColumn & /*dest*/, ObjectIterator & /*iterator*/, ASTPtr & /*query_ptr*/, DialectType /*dialect_type*/) requires IsObjectIterator<Iterator>
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JSON_QUERY is not implemented for Object or Tuple.");
    }
};

template <typename Iterator>
class SQLJSONLengthImpl
{
public:

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, Iterator & iterator, ASTPtr & query_ptr, DialectType dialect_type) requires IsElementIterator<Iterator>
    {
        using Element = typename Iterator::Element;
        using JSONParser = typename Iterator::JSONParserType;
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = iterator.getElement();
        VisitorStatus status;

        ColumnNullable & col = assert_cast<ColumnNullable &>(dest);
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = iterator.getElement();
        }

        if (status == VisitorStatus::Exhausted)
        {
            col.insertData(nullptr, 0);
            return false;
        }

        size_t size;
        if (current_element.isArray())
            size = current_element.getArray().size();
        else if (current_element.isObject())
            size = current_element.getObject().size();
        else
        {
            if (dialect_type == DialectType::MYSQL)
                size = 0;
            else
                size = 1;
        }

        col.insert(size);
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator, ASTPtr & query_ptr, DialectType dialect_type) requires IsObjectIterator<Iterator>
    {
        ColumnNullable & col = assert_cast<ColumnNullable &>(dest);
        ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
        }

        if (status == VisitorStatus::Exhausted)
        {
            col.insertData(nullptr, 0);
            return false;
        }

        const auto * column_array = typeid_cast<const ColumnArray *>(iterator.getColumn().get());
        if (column_array)
        {
            const auto & offsets = column_array->getOffsets();
            auto row = iterator.getRow();
            UInt64 size = offsets[row] - offsets[row - 1];
            col.insert(size);
            return true;
        }

        const auto * column_tuple = typeid_cast<const ColumnTuple *>(iterator.getColumn().get());
        if (column_tuple)
        {
            if (isDummyTuple(*iterator.getType()))
                return false;

            UInt64 size = column_tuple->getColumns().size();
            col.insert(size);
            return true;
        }

        if (dialect_type == DialectType::MYSQL)
        {
            col.insert(0);
        }
        else
        {
            col.insert(1);
        }

        return true;
    }
};

template <typename Name>
class FunctionSQLJSONContains : public IFunction, WithConstContext
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionSQLJSONContains>(context_); }
    explicit FunctionSQLJSONContains(ContextPtr context_) : WithConstContext(context_)
    {
        func_compare = FunctionFactory::instance().get("equals", getContext());
        func_array_has = FunctionFactory::instance().get("has", getContext());
        func_array_hasSubStr = FunctionFactory::instance().get("hasSubstr", getContext());
    }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return std::make_shared<DataTypeUInt8>(); }

    bool tupleContains(const ColumnsWithTypeAndName & compare_columns) const
    {
        auto target_column = compare_columns[0];
        auto candidate_column = compare_columns[1];

        if (isTuple(target_column.type) && isTuple(candidate_column.type))
        {
            const auto & target_tuple_column = assert_cast<const ColumnTuple &>(*target_column.column);
            const auto & target_tuple_type = assert_cast<const DataTypeTuple &>(*target_column.type);


            const auto & candidate_tuple_column = assert_cast<const ColumnTuple &>(*candidate_column.column);
            const auto & candidate_type_tuple = assert_cast<const DataTypeTuple &>(*candidate_column.type);

            const auto & candidate_tuple_element_names = candidate_type_tuple.getElementNames();
            const auto & candidate_tuple_elements = candidate_type_tuple.getElements();

            const auto & target_tuple_element_names = target_tuple_type.getElementNames();
            const auto & target_tuple_elements = target_tuple_type.getElements();

            bool contains_all = false;
            for (size_t i = 0; i < candidate_tuple_element_names.size(); i++)
            {
                auto candidate_tuple_element_name = candidate_tuple_element_names[i];
                bool contains = false;
                for (size_t j = 0; j < target_tuple_element_names.size(); j++)
                {
                    auto target_tuple_element_name = target_tuple_element_names[j];
                    if (candidate_tuple_element_name == target_tuple_element_name)
                    {
                        contains = tupleContains(
                            {{target_tuple_column.getColumnPtr(j), target_tuple_elements[j], "_dummy"},
                             {candidate_tuple_column.getColumnPtr(i), candidate_tuple_elements[i], "_dummy"}});
                    }
                }

                if (!contains)
                {
                    contains_all = false;
                    break;
                }
                else
                {
                    contains_all = true;
                }
            }

            return contains_all;
        }
        else if (
            (isArray(target_column.type) && isArray(candidate_column.type))
            || (isNumberOrString(target_column.type) && isNumberOrString(candidate_column.type)))
        {
            auto compare_result_column
                = func_compare->build(compare_columns)->execute(compare_columns, std::make_shared<DataTypeUInt8>(), 1);
            return compare_result_column->getBool(0);
        }

        return false;
    }

    bool
    insertResultToColumn(IColumn & dest, ObjectIterator & iterator, const ColumnWithTypeAndName & candidate, ASTPtr & query_ptr) const
    {
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);

        if (query_ptr)
        {
            ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
            VisitorStatus status;
            while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
            {
                if (status == VisitorStatus::Ok)
                {
                    break;
                }
            }

            if (status == VisitorStatus::Exhausted)
            {
                return false;
            }
        }

        auto extract_tree = JSONExtractTree<ObjectIterator>::build(Name::name, iterator.getType());

        MutableColumnPtr extract_item{iterator.getType()->createColumn()};
        extract_item->reserve(1);
        extract_tree->insertResultToColumn(*extract_item, iterator);

        ColumnsWithTypeAndName compare_columns{{std::move(extract_item), iterator.getType(), "json_extract_item"}, candidate};

        if (isArray(iterator.getType()) && isArray(candidate.type))
        {
            // Handle array type, check whether has candidate sub array in array

            auto array_has_substr_result_column = func_array_hasSubStr->build(compare_columns)->execute(compare_columns, std::make_shared<DataTypeUInt8>(), 1);
            col_bool.insertFrom(*array_has_substr_result_column, 0);
        }
        else if (isArray(iterator.getType()) && isNumberOrString(candidate.type))
        {
            // Handle array type, check whether has candidate in array
            auto array_has_result_column = func_array_has->build(compare_columns)->execute(compare_columns, std::make_shared<DataTypeUInt8>(), 1);
            col_bool.insertFrom(*array_has_result_column, 0);
        }
        else if (isArray(iterator.getType()) && isObject(candidate.type))
            return false;
        else if (isTuple(iterator.getType()) && isObject(candidate.type))
        {
            const auto& candidate_const_column = assert_cast<const ColumnConst &>(*candidate.column);
            ColumnPtr candidate_tuple_column;
            DataTypePtr candidate_type_tuple;
            const auto& candidate_object_column = assert_cast<const ColumnObject &>(candidate_const_column.getDataColumn());
            std::tie(candidate_tuple_column, candidate_type_tuple) = unflattenObjectToTuple(candidate_object_column);
            col_bool.insert(tupleContains({compare_columns[0], {candidate_tuple_column, candidate_type_tuple, "candidate_object"}}) ? 1 : 0);
        }
        else if (isTuple(iterator.getType()) && isArray(candidate.type))
            return false;
        else if (isTuple(iterator.getType()) && isNumberOrString(candidate.type))
            return false;
        else
        {
            // Handle primitive type , compare directly
            auto compare_result_column
                = func_compare->build(compare_columns)->execute(compare_columns, std::make_shared<DataTypeUInt8>(), 1);
            col_bool.insertFrom(*compare_result_column, 0);
        }

        return true;
    }

    bool
    insertResultToColumn(IColumn & dest, ElementIterator<SimdJSONParser> & iterator, const ColumnWithTypeAndName & candidate, ASTPtr & query_ptr) const
    {
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);

        auto current_element = iterator.getElement();
        if (query_ptr)
        {
            GeneratorJSONPath<SimdJSONParser> generator_json_path(query_ptr);
            VisitorStatus status;
            while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
            {
                if (status == VisitorStatus::Ok)
                {
                    break;
                }
                current_element = iterator.getElement();
            }

            if (status == VisitorStatus::Exhausted)
            {
                return false;
            }
        }

        const auto & candidate_json_column = candidate.column;
        const auto * candidate_json_const = typeid_cast<const ColumnConst *>(candidate_json_column.get());
        const auto * candidate_json_string = typeid_cast<const ColumnString *>(
            candidate_json_const ? candidate_json_const->getDataColumnPtr().get() : candidate_json_column.get());

        std::string_view json{candidate_json_string ? candidate_json_string->getDataAt(0) : ""};
        SimdJSONParser json_parser;
        using Element = typename SimdJSONParser::Element;
        Element sub_document;
        const bool parse_ok = json_parser.parse(json, sub_document);

        if (parse_ok)
        {
            bool contains = JSONUtils<SimdJSONParser>::contains(current_element, sub_document);
            col_bool.insert(contains ? 1 : 0);
        }

        return parse_ok;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        //Only support Object JSON

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", Name::name);

        // TODO: add logic to handle single argument
        if (arguments.size() < 2)
        {
            throw Exception{"JSONPath functions require at least 2 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
        }

        const auto & json_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(json_column.type));

        bool is_string = isString(first_type_base);
        bool is_object = isObject(first_type_base);
        bool is_tuple = isTuple(first_type_base);

        if (!is_string && !is_object && !is_tuple)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing Object or Tuple or JSON, illegal type: {}",
                Name::name,
                json_column.type->getName());

        const auto & candidate = arguments[1];
        if (!isColumnConst(*candidate.column))
            throw Exception("Second argument (Candidate) must be constant", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        MutableColumnPtr to{result_type->createColumn()};
        to->reserve(input_rows_count);

        ASTPtr res;
        if (arguments.size() == 3)
        {
            const auto & json_path_column = arguments[2];

            if (!isString(json_path_column.type))
            {
                throw Exception(
                    "JSONPath functions require third argument to be JSONPath of type string, illegal type: "
                        + json_path_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            if (!isColumnConst(*json_path_column.column))
            {
                throw Exception("Third argument (JSONPath) must be constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const ColumnPtr & arg_jsonpath = json_path_column.column;
            const auto * arg_jsonpath_const = typeid_cast<const ColumnConst *>(arg_jsonpath.get());
            const auto * arg_jsonpath_string = typeid_cast<const ColumnString *>(arg_jsonpath_const->getDataColumnPtr().get());

            /// Get data and offsets for 1 argument (JSONPath)
            const ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Prepare to parse 1 argument (JSONPath)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            uint32_t parse_depth = getContext()->getSettingsRef().max_parser_depth;
            IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            Expected expected;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
            }
        }

        const auto & arg_json = json_column.column;
        const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());

        if (is_object || is_tuple)
        {
            const auto * col_json_object
                = typeid_cast<const ColumnObject *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_object)
                throw Exception{ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", arg_json->getName()};

            ColumnPtr column_tuple;
            DataTypePtr type_tuple;

            if (is_object)
            {
                std::tie(column_tuple, type_tuple) = unflattenObjectToTuple(*col_json_object);
            }
            else
            {
                column_tuple = col_json_object->getPtr();
                type_tuple = json_column.type;
            }
            for (const auto i : collections::range(0, input_rows_count))
            {
                ObjectIterator iterator(type_tuple, column_tuple, col_json_const ? 0 : i);

                bool added_to_column = this->insertResultToColumn(*to, iterator, candidate, res);
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
        }
        else
        {
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            SimdJSONParser json_parser;
            using Element = typename SimdJSONParser::Element;
            Element document;
            bool document_ok = false;

            for (const auto i : collections::range(0, input_rows_count))
            {
                std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                ElementIterator<SimdJSONParser> iterator(document);
                if (document_ok)
                {
                    added_to_column = this->insertResultToColumn(*to, iterator, candidate, res);
                }
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
        }

        return to;
    }

private:
    mutable std::shared_ptr<IFunctionOverloadResolver> func_compare;
    mutable std::shared_ptr<IFunctionOverloadResolver> func_array_has;
    mutable std::shared_ptr<IFunctionOverloadResolver> func_array_hasSubStr;
};

template <typename Name>
class FunctionSQLJSONContainsPath : public IFunction, WithConstContext
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionSQLJSONContainsPath>(context_); }
    explicit FunctionSQLJSONContainsPath(ContextPtr context_) : WithConstContext(context_) { }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return std::make_shared<DataTypeUInt8>(); }

    bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator, ASTs & query_ptrs, bool contains_all) const
    {
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);

        bool contains = false;

        for (const auto & query_ptr : query_ptrs)
        {
            auto temp_iterator = iterator;
            ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
            VisitorStatus status;
            while ((status = generator_json_path.getNextItem(temp_iterator)) != VisitorStatus::Exhausted)
            {
                if (status == VisitorStatus::Ok)
                {
                    contains = true;
                    break;
                }
            }

            if (status == VisitorStatus::Exhausted)
            {
                if (contains_all)
                    return false;
                else
                    continue;
            }
        }

        col_bool.insert(contains ? 1 : 0);
        return true;
    }

    bool insertResultToColumn(IColumn & dest, ElementIterator<SimdJSONParser> & iterator, ASTs & query_ptrs, bool contains_all) const
    {
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);

        bool contains = false;
        for (const auto & query_ptr : query_ptrs)
        {
            GeneratorJSONPath<SimdJSONParser> generator_json_path(query_ptr);
            auto current_element = iterator.getElement();
            VisitorStatus status;

            while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
            {
                if (status == VisitorStatus::Ok)
                {
                    contains = true;
                    break;
                }
                current_element = iterator.getElement();
            }

            if (status == VisitorStatus::Exhausted)
            {
                if (contains_all)
                    return false;
                else
                    continue;
            }
        }

        col_bool.insert(contains ? 1 : 0);
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        //Only support Object JSON

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", Name::name);

        // TODO: add logic to handle single argument
        if (arguments.size() < 3)
        {
            throw Exception{"Function JSON_CONTAINS_PATH require at least 3 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
        }

        const auto & json_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(json_column.type));

        bool is_string = isString(first_type_base);
        bool is_object = isObject(first_type_base);
        bool is_tuple = isTuple(first_type_base);

        if (!is_string && !is_object && !is_tuple)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing Object or Tuple or JSON, illegal type: {}",
                Name::name,
                json_column.type->getName());

        const auto & any_or_all_column = arguments[1];
        if (!isString(any_or_all_column.type) || !isColumnConst(*any_or_all_column.column))
            throw Exception("Second argument (any or all) must be constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const ColumnPtr & arg_any_or_all = any_or_all_column.column;
        const auto * arg_any_or_all_const = typeid_cast<const ColumnConst *>(arg_any_or_all.get());
        const auto * arg_any_or_all_string = typeid_cast<const ColumnString *>(arg_any_or_all_const->getDataColumnPtr().get());

        bool contains_all = false;
        if (Poco::toLower(arg_any_or_all_string->getDataAt(0).toString()) == "all")
            contains_all = true;
        else if (Poco::toLower(arg_any_or_all_string->getDataAt(0).toString()) == "one")
            contains_all = false;
        else
            throw Exception("Second argument (any or all) only can be any or all", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        ASTs json_path_ast_ptrs;
        for (size_t i = 2; i < arguments.size(); i++)
        {
            const auto & json_path_column = arguments[i];

            if (!isString(json_path_column.type))
            {
                throw Exception(
                    "JSONPath functions require third argument to be JSONPath of type string, illegal type: "
                        + json_path_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const ColumnPtr & arg_jsonpath = json_path_column.column;
            const auto * arg_jsonpath_string = typeid_cast<const ColumnString *>(arg_jsonpath.get());

            /// Get data and offsets for 1 argument (JSONPath)
            const ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Prepare to parse 1 argument (JSONPath)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            uint32_t parse_depth = getContext()->getSettingsRef().max_parser_depth;
            IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
            }

            json_path_ast_ptrs.emplace_back(res);
        }

        MutableColumnPtr to{result_type->createColumn()};
        to->reserve(input_rows_count);

        const auto & arg_json = json_column.column;
        const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());

        if (is_object || is_tuple)
        {
            const auto * col_json_object
                = typeid_cast<const ColumnObject *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_object)
                throw Exception{ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", arg_json->getName()};

            ColumnPtr column_tuple;
            DataTypePtr type_tuple;

            if (is_object)
            {
                std::tie(column_tuple, type_tuple) = unflattenObjectToTuple(*col_json_object);
            }
            else
            {
                column_tuple = col_json_object->getPtr();
                type_tuple = json_column.type;
            }


            for (const auto i : collections::range(0, input_rows_count))
            {
                ObjectIterator iterator(type_tuple, column_tuple, col_json_const ? 0 : i);

                bool added_to_column = this->insertResultToColumn(*to, iterator, json_path_ast_ptrs, contains_all);
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
        }
        else
        {
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            SimdJSONParser json_parser;
            using Element = typename SimdJSONParser::Element;
            Element document;
            bool document_ok = false;

            for (const auto i : collections::range(0, input_rows_count))
            {
                std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                ElementIterator<SimdJSONParser> iterator(document);
                if (document_ok)
                {
                    added_to_column = this->insertResultToColumn(*to, iterator, json_path_ast_ptrs, contains_all);
                }
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
        }

        return to;
    }
};

template <typename Name>
class FunctionSQLJSONArrayContains : public IFunction, WithConstContext
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionSQLJSONArrayContains>(context_); }
    explicit FunctionSQLJSONArrayContains(ContextPtr context_) : WithConstContext(context_)
    {}

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    template<typename JSONParser>
    ColumnPtr internalExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t /*parse_depth*/) const
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", Name::name);

        const auto & json_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(json_column.type));

        bool is_string = isString(first_type_base);

        if (!is_string)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string, illegal type: {}",
                Name::name,
                json_column.type->getName());


        const ColumnPtr & arg_json = json_column.column;
        const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
        const auto * col_json_string
            = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());
        /// Get data and offsets for 2 argument (JSON)
        const ColumnString::Chars & chars_json = col_json_string->getChars();
        const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

        const auto & target_value_column = arguments[1];

        JSONParser json_parser;
        using Element = typename JSONParser::Element;
        Element document;
        bool document_ok = false;

        MutableColumnPtr to{result_type->createColumn()};
        to->reserve(input_rows_count);

        auto compare = [&target_value_column](const Element & json_array_element)
        {
            if (isString(target_value_column.type) && json_array_element.isString())
            {
                return target_value_column.column->getDataAt(0).toString() == json_array_element.getString();
            }

            if (isNumber(target_value_column.type) && json_array_element.isInt64())
            {
                return target_value_column.column->getInt(0) == json_array_element.getInt64();
            }

            if (isBool(target_value_column.type) && json_array_element.isBool())
                return target_value_column.column->getBool(0) == json_array_element.getBool();

            return false;
        };

        for (const auto i : collections::range(0, input_rows_count))
        {

            std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
            document_ok = json_parser.parse(json, document);
            if (!document_ok)
            {
                to->insertDefault();
                continue;
            }

            if (document.isArray())
            {
                const auto & json_array = document.getArray();
                for (auto it = json_array.begin(); it != json_array.end(); ++it)
                {
                    if (compare(*it))
                    {
                        to->insert(1);
                        break;
                    }
                }
                to->insertDefault();
            }
            else
            {
                to->insertDefault();
            }
        }

        return to;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Choose JSONParser.
        /// 1. Lexer(path) -> Tokens
        /// 2. Create ASTPtr
        /// 3. Parser(Tokens, ASTPtr) -> complete AST
        /// 4. Execute functions: call getNextItem on generator and handle each item
        uint32_t parse_depth = getContext()->getSettingsRef().max_parser_depth;
#if USE_SIMDJSON
        if (getContext()->getSettingsRef().allow_simdjson)
            return this->template internalExecuteImpl<SimdJSONParser>(arguments, result_type, input_rows_count, parse_depth);
#endif
        return this->template internalExecuteImpl<DummyJSONParser>(arguments, result_type, input_rows_count, parse_depth);
    }
};

}
