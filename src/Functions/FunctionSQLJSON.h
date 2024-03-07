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
#include "Common/assert_cast.h"
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <common/range.h>
#include "Columns/ColumnObject.h"
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/IDataType.h"
#include "Functions/FunctionsComparison.h"
#include "Parsers/IAST_fwd.h"
#include <Functions/FunctionFactory.h>

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

class FunctionSQLJSONHelpers
{
public:
    template <typename Name, template <typename> typename Impl, class JSONParser>
    class Executor
    {
    public:
        static ColumnPtr
        run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);
            // TODO: add logic to handle single argument
            if (arguments.size() < 2)
            {
                throw Exception{"JSONPath functions require at least 2 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
            }

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
                throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
            }

            /// Get data and offsets for 2 argument (JSON)
            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl<JSONParser> impl;

            for (const auto i : collections::range(0, input_rows_count))
            {
                std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                if (document_ok)
                {
                    added_to_column = impl.insertResultToColumn(*to, document, res);
                }
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
            return to;
        }
    };
};

template <typename Name, template <typename> typename Impl>
class FunctionSQLJSON : public IFunction, WithConstContext
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionSQLJSON>(context_); }
    explicit FunctionSQLJSON(ContextPtr context_) : WithConstContext(context_) { }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return Impl<DummyJSONParser>::getReturnType(Name::name, arguments);
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
            return FunctionSQLJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count, parse_depth);
#endif
        return FunctionSQLJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count, parse_depth);
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

template <typename JSONParser>
class SQLJSONExistsImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = root;
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

template <typename JSONParser>
class SQLJSONValueImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
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
            current_element = root;
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
};

/**
 * Function to test jsonpath member access, will be removed in final PR
 * @tparam JSONParser parser
 */
template <typename JSONParser>
class SQLJSONQueryImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
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
            current_element = root;
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
};

template <typename JSONParser>
class SQLJSONLengthImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        VisitorStatus status;

        ColumnNullable & col = assert_cast<ColumnNullable &>(dest);
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = root;
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
            size = 1;

        col.insert(size);
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        //Only support Object JSON
        bool has_nothing_argument = false;
        for (const auto & arg : arguments)
            has_nothing_argument |= isNothing(arg.type);

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", Name::name);

        // TODO: add logic to handle single argument
        if (arguments.size() < 2)
        {
            throw Exception{"JSONPath functions require at least 2 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
        }

        const auto & json_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(json_column.type));

        bool is_object = isObject(first_type_base);
        bool is_tuple = isTuple(first_type_base);

        if (!is_object && !is_tuple)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing Object or Tuple, illegal type: {}",
                Name::name,
                json_column.type->getName());

        const auto & candidate = arguments[1];
        if (!isColumnConst(*candidate.column))
            throw Exception("Second argument (Candidate) must be constant", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        MutableColumnPtr to{result_type->createColumn()};
        to->reserve(input_rows_count);

        const auto & arg_json = json_column.column;
        const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
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

        for (const auto i : collections::range(0, input_rows_count))
        {
            ObjectIterator iterator(type_tuple, column_tuple, col_json_const ? 0 : i);

            bool added_to_column = this->insertResultToColumn(*to, iterator, candidate, res);
            if (!added_to_column)
            {
                to->insertDefault();
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
            ObjectJSONGeneratorJSONPath generator_json_path(query_ptr);
            VisitorStatus status;
            while ((status = generator_json_path.getNextItem(iterator)) != VisitorStatus::Exhausted)
            {
                if (status == VisitorStatus::Ok)
                {
                    if (!contains_all)
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        //Only support Object JSON
        bool has_nothing_argument = false;
        for (const auto & arg : arguments)
            has_nothing_argument |= isNothing(arg.type);

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", Name::name);

        // TODO: add logic to handle single argument
        if (arguments.size() < 3)
        {
            throw Exception{"Function JSON_CONTAINS_PATH require at least 3 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
        }

        const auto & json_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(json_column.type));

        bool is_object = isObject(first_type_base);
        bool is_tuple = isTuple(first_type_base);

        if (!is_object && !is_tuple)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing Object or Tuple, illegal type: {}",
                Name::name,
                json_column.type->getName());

        const auto & any_or_all_column = arguments[1];
        if (!isString(any_or_all_column.type) || !isColumnConst(*any_or_all_column.column))
            throw Exception("Second argument (any or all) must be constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        
        MutableColumnPtr to{result_type->createColumn()};
        to->reserve(input_rows_count);

        const auto & arg_json = json_column.column;
        const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
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

        for (const auto i : collections::range(0, input_rows_count))
        {
            ObjectIterator iterator(type_tuple, column_tuple, col_json_const ? 0 : i);

            bool added_to_column = this->insertResultToColumn(*to, iterator, json_path_ast_ptrs, contains_all);
            if (!added_to_column)
            {
                to->insertDefault();
            }
        }

        return to;
    }
};
}
