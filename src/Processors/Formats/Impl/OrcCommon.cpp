#include "OrcCommon.h"
#include <typeinfo>
#include "Common/Exception.h"
#include "Common/ProfileEvents.h"
#include "Columns/ColumnLowCardinality.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/Types.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "Storages/IStorage.h"
#if USE_ORC
#    include <Columns/ColumnDecimal.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsDateTime.h>
#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate32.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/insertNullAsDefaultIfNeeded.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/WriteHelpers.h>
#    include <IO/copyData.h>
#    include <Interpreters/castColumn.h>
#    include <arrow/result.h>
#    include <arrow/status.h>
#    include <boost/algorithm/string/case_conv.hpp>
#    include "ArrowBufferedStreams.h"
#    include "ArrowColumnCache.h"
#    include "IO/WithFileSize.h"


namespace ProfileEvents
{
extern const Event OrcReadDirectCount;
extern const Event OrcReadCacheCount;
extern const Event OrcIOMergedBytes;
extern const Event OrcIOMergedCount;
extern const Event OrcIOSharedBytes;
extern const Event OrcIOSharedCount;
extern const Event OrcIODirectBytes;
extern const Event OrcIODirectCount;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int THERE_IS_NO_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_READ_ALL_DATA;
}

static const orc::Type * getORCTypeByName(const orc::Type & schema, const String & name, bool ignore_case)
{
    for (UInt64 i = 0; i != schema.getSubtypeCount(); ++i)
        if (boost::equals(schema.getFieldName(i), name) || (ignore_case && boost::iequals(schema.getFieldName(i), name)))
            return schema.getSubtype(i);
    return nullptr;
}

DataTypePtr parseORCType(const orc::Type * orc_type, bool skip_columns_with_unsupported_types, bool & skipped)
{
    assert(orc_type != nullptr);

    const int subtype_count = static_cast<int>(orc_type->getSubtypeCount());
    switch (orc_type->getKind())
    {
        case orc::TypeKind::BOOLEAN:
            return DataTypeFactory::instance().get("Bool");
        case orc::TypeKind::BYTE:
            return std::make_shared<DataTypeInt8>();
        case orc::TypeKind::SHORT:
            return std::make_shared<DataTypeInt16>();
        case orc::TypeKind::INT:
            return std::make_shared<DataTypeInt32>();
        case orc::TypeKind::LONG:
            return std::make_shared<DataTypeInt64>();
        case orc::TypeKind::FLOAT:
            return std::make_shared<DataTypeFloat32>();
        case orc::TypeKind::DOUBLE:
            return std::make_shared<DataTypeFloat64>();
        case orc::TypeKind::DATE:
            return std::make_shared<DataTypeDate32>();
        case orc::TypeKind::TIMESTAMP:
            return std::make_shared<DataTypeDateTime64>(9);
        case orc::TypeKind::TIMESTAMP_INSTANT:
            return std::make_shared<DataTypeDateTime64>(9, "UTC");
        case orc::TypeKind::VARCHAR:
        case orc::TypeKind::BINARY:
        case orc::TypeKind::STRING:
            return std::make_shared<DataTypeString>();
        case orc::TypeKind::CHAR:
            return std::make_shared<DataTypeFixedString>(orc_type->getMaximumLength());
        case orc::TypeKind::DECIMAL: {
            UInt64 precision = orc_type->getPrecision();
            UInt64 scale = orc_type->getScale();
            if (precision == 0)
            {
                // In HIVE 0.11/0.12 precision is set as 0, but means max precision
                return createDecimal<DataTypeDecimal>(38, 6);
            }
            else
                return createDecimal<DataTypeDecimal>(precision, scale);
        }
        case orc::TypeKind::LIST: {
            if (subtype_count != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc List type {}", orc_type->toString());

            DataTypePtr nested_type = parseORCType(orc_type->getSubtype(0), skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            return std::make_shared<DataTypeArray>(nested_type);
        }
        case orc::TypeKind::MAP: {
            if (subtype_count != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc Map type {}", orc_type->toString());

            DataTypePtr key_type = parseORCType(orc_type->getSubtype(0), skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            DataTypePtr value_type = parseORCType(orc_type->getSubtype(1), skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            return std::make_shared<DataTypeMap>(key_type, value_type);
        }
        case orc::TypeKind::STRUCT: {
            DataTypes nested_types;
            Strings nested_names;
            nested_types.reserve(subtype_count);
            nested_names.reserve(subtype_count);

            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                auto parsed_type = parseORCType(orc_type->getSubtype(i), skip_columns_with_unsupported_types, skipped);
                if (skipped)
                    return {};

                nested_types.push_back(parsed_type);
                nested_names.push_back(orc_type->getFieldName(i));
            }
            return std::make_shared<DataTypeTuple>(nested_types, nested_names);
        }
        default: {
            if (skip_columns_with_unsupported_types)
            {
                skipped = true;
                return {};
            }

            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported ORC type '{}'."
                "If you want to skip columns with unsupported types, "
                "you can enable setting input_format_orc_skip_columns_with_unsupported_types_in_schema_inference",
                orc_type->toString());
        }
    }
}

static std::optional<orc::PredicateDataType> convertORCTypeToPredicateType(const orc::Type & orc_type)
{
    switch (orc_type.getKind())
    {
        case orc::BOOLEAN:
            return orc::PredicateDataType::BOOLEAN;
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG:
            return orc::PredicateDataType::LONG;
        case orc::FLOAT:
        case orc::DOUBLE:
            return orc::PredicateDataType::FLOAT;
        case orc::VARCHAR:
        case orc::CHAR:
        case orc::STRING:
            return orc::PredicateDataType::STRING;
        case orc::DATE:
            return orc::PredicateDataType::DATE;
        case orc::TIMESTAMP:
            return orc::PredicateDataType::TIMESTAMP;
        case orc::DECIMAL:
            return orc::PredicateDataType::DECIMAL;
        default:
            return {};
    }
}

static String getColumnNameFromKeyCondition(const KeyCondition & key_condition, size_t indice)
{
    const auto & key_columns = key_condition.getKeyColumns();
    for (const auto & [name, i] : key_columns)
    {
        if (i == indice)
            return name;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't get column from KeyCondition with indice {}", indice);
}

static std::optional<orc::Literal>
convertFieldToORCLiteral(const orc::Type & orc_type, const Field & field, DataTypePtr type_hint = nullptr)
{
    try
    {
        /// We always fallback to return null if possible CH type hint not consistent with ORC type
        switch (orc_type.getKind())
        {
            case orc::BOOLEAN: {
                /// May throw exception
                auto val = field.get<UInt64>();
                return orc::Literal(val != 0);
            }
            case orc::BYTE:
            case orc::SHORT:
            case orc::INT:
            case orc::LONG: {
                /// May throw exception
                auto val = field.get<Int64>();
                return orc::Literal(val);
            }
            case orc::FLOAT:
            case orc::DOUBLE: {
                Float64 val;
                if (field.tryGet(val))
                    return orc::Literal(val);
                break;
            }
            case orc::VARCHAR:
            case orc::CHAR:
            case orc::STRING: {
                String str;
                if (field.tryGet(str))
                    return orc::Literal(str.data(), str.size());
                break;
            }
            case orc::DATE: {
                Int64 val;
                if (field.tryGet(val))
                    return orc::Literal(orc::PredicateDataType::DATE, val);
                break;
            }
            case orc::TIMESTAMP: {
                if (type_hint && isDateTime64(type_hint))
                {
                    const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(type_hint.get());
                    if (datetime64_type->getScale() != 9)
                        return std::nullopt;
                }

                DecimalField<Decimal64> ts;
                if (field.tryGet(ts))
                {
                    Int64 secs = (ts.getValue() / ts.getScaleMultiplier()).convertTo<Int64>();
                    Int32 nanos = (ts.getValue() - (ts.getValue() / ts.getScaleMultiplier()) * ts.getScaleMultiplier()).convertTo<Int32>();
                    return orc::Literal(secs, nanos);
                }
                break;
            }
            case orc::DECIMAL: {
                auto precision = orc_type.getPrecision();
                if (precision == 0)
                    precision = 38;

                if (precision <= DecimalUtils::max_precision<Decimal32>)
                {
                    DecimalField<Decimal32> val;
                    if (field.tryGet(val))
                    {
                        Int64 right = val.getValue().convertTo<Int64>();
                        return orc::Literal(
                            orc::Int128(right), static_cast<Int32>(orc_type.getPrecision()), static_cast<Int32>(orc_type.getScale()));
                    }
                }
                else if (precision <= DecimalUtils::max_precision<Decimal64>)
                {
                    DecimalField<Decimal64> val;
                    if (field.tryGet(val))
                    {
                        Int64 right = val.getValue().convertTo<Int64>();
                        return orc::Literal(
                            orc::Int128(right), static_cast<Int32>(orc_type.getPrecision()), static_cast<Int32>(orc_type.getScale()));
                    }
                }
                else if (precision <= DecimalUtils::max_precision<Decimal128>)
                {
                    DecimalField<Decimal128> val;
                    if (field.tryGet(val))
                    {
                        Int64 high = val.getValue().value.items[1];
                        UInt64 low = static_cast<UInt64>(val.getValue().value.items[0]);
                        return orc::Literal(
                            orc::Int128(high, low), static_cast<Int32>(orc_type.getPrecision()), static_cast<Int32>(orc_type.getScale()));
                    }
                }
                break;
            }
            default:
                break;
        }
        return std::nullopt;
    }
    catch (Exception &)
    {
        return std::nullopt;
    }
}

/// Attention: evaluateRPNElement is only invoked in buildORCSearchArgumentImpl.
/// So it is guaranteed that:
///     1. elem has no monotonic_functions_chains.
///     2. if elem function is FUNCTION_IN_RANGE/FUNCTION_NOT_IN_RANGE, `set_index` is not null and `set_index->getOrderedSet().size()` is 1.
///     3. elem function should be FUNCTION_IN_RANGE/FUNCTION_NOT_IN_RANGE/FUNCTION_IN_SET/FUNCTION_NOT_IN_SET/FUNCTION_IS_NULL/FUNCTION_IS_NOT_NULL
static bool evaluateRPNElement(const Field & field, const KeyCondition::RPNElement & elem)
{
    Range key_range(field);
    switch (elem.function)
    {
        case KeyCondition::RPNElement::FUNCTION_IN_RANGE:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE: {
            /// Rows with null values should never output when filters like ">=", ">", "<=", "<", '=' are applied
            if (field.isNull())
                return false;

            bool res = elem.range.intersectsRange(key_range);
            if (elem.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE)
                res = !res;
            return res;
        }
        case KeyCondition::RPNElement::FUNCTION_IN_SET:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_SET: {
            const auto & set_index = elem.set_index;
            const auto & ordered_set = set_index->getOrderedSet();
            const auto & set_column = ordered_set[0];

            bool res = false;
            for (size_t i = 0; i < set_column->size(); ++i)
            {
                if (Range::equals(field, (*set_column)[i]))
                {
                    res = true;
                    break;
                }
            }
            if (elem.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET)
                res = !res;
            return res;
        }
        case KeyCondition::RPNElement::FUNCTION_IS_NULL:
        case KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL: {
            if (field.isNull())
                return elem.function == KeyCondition::RPNElement::FUNCTION_IS_NULL;
            else
                return elem.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected RPNElement Function {}", elem.toString());
    }
}

DataTypePtr makeNullableRecursively(DataTypePtr type)
{
    if (!type)
        return nullptr;

    WhichDataType which(type);

    if (which.isNullable())
        return type;

    if (which.isArray())
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        auto nested_type = makeNullableRecursively(array_type->getNestedType());
        return nested_type ? std::make_shared<DataTypeArray>(nested_type) : nullptr;
    }

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        DataTypes nested_types;
        for (const auto & element : tuple_type->getElements())
        {
            auto nested_type = makeNullableRecursively(element);
            if (!nested_type)
                return nullptr;
            nested_types.push_back(nested_type);
        }

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(std::move(nested_types), tuple_type->getElementNames());

        return std::make_shared<DataTypeTuple>(std::move(nested_types));
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = makeNullableRecursively(map_type->getKeyType());
        auto value_type = makeNullableRecursively(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(removeNullable(key_type), value_type) : nullptr;
    }

    if (which.isLowCardinality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = makeNullableRecursively(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }


    return makeNullable(type);
}

static void buildORCSearchArgumentImpl(
    const KeyCondition & key_condition,
    const Block & header,
    const orc::Type & schema,
    KeyCondition::RPN & rpn_stack,
    orc::SearchArgumentBuilder & builder,
    const FormatSettings & format_settings)
{
    if (rpn_stack.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty rpn stack in buildORCSearchArgumentImpl");

    const auto & curr = rpn_stack.back();
    switch (curr.function)
    {
        case KeyCondition::RPNElement::FUNCTION_IN_RANGE:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE:
        case KeyCondition::RPNElement::FUNCTION_IN_SET:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_SET:
        case KeyCondition::RPNElement::FUNCTION_IS_NULL:
        case KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL: {
            const bool need_wrap_not = curr.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
            const bool contains_is_null = curr.function == KeyCondition::RPNElement::FUNCTION_IS_NULL
                || curr.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL;
            const bool contains_in_set = curr.function == KeyCondition::RPNElement::FUNCTION_IN_SET
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
            const bool contains_in_range = curr.function == KeyCondition::RPNElement::FUNCTION_IN_RANGE
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE;

            SCOPE_EXIT({ rpn_stack.pop_back(); });


            /// Key filter expressions like "func(col) > 100" are not supported for ORC filter push down
            if (!curr.monotonic_functions_chain.empty())
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            /// key filter expressions like "(a, b, c) in " or "(func(a), b) in " are not supported for ORC filter push down
            /// Only expressions like "a in " are supported currently, maybe we can improve it later.
            auto set_index = curr.set_index;
            if (contains_in_set)
            {
                if (!set_index || set_index->getOrderedSet().size() != 1 || set_index->hasMonotonicFunctionsChain())
                {
                    builder.literal(orc::TruthValue::YES_NO_NULL);
                    break;
                }
            }

            String column_name = getColumnNameFromKeyCondition(key_condition, curr.key_column);
            const auto * orc_type = getORCTypeByName(schema, column_name, format_settings.orc.case_insensitive_column_matching);
            if (!orc_type)
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            /// Make sure key column in header has exactly the same type with key column in ORC file schema
            /// Counter-example 1:
            ///     Column a has type "Nullable(Int64)" in ORC file, but in header column a has type "Int64", which is allowed in CH.
            ///     For queries with where condition like "a is null", if a column contains null value, pushing or not pushing down filters
            ///     would result in different outputs.
            /// Counter-example 2:
            ///     Column a has type "Nullable(Int64)" in ORC file, but in header column a has type "Nullable(UInt64)".
            ///     For queries with where condition like "a > 10", if a column contains negative values such as "-1", pushing or not pushing
            ///     down filters would result in different outputs.
            bool skipped = false;
            auto expect_type = makeNullableRecursively(parseORCType(orc_type, true, skipped));
            const ColumnWithTypeAndName * column = header.findByName(column_name, format_settings.orc.case_insensitive_column_matching);
            if (!expect_type || !column)
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            auto nested_type = removeNullable(recursiveRemoveLowCardinality(column->type));
            auto expect_nested_type = removeNullable(expect_type);
            if (!nested_type->equals(*expect_nested_type))
            {
                LOG_DEBUG(
                    getLogger(__PRETTY_FUNCTION__),
                    "failed to pushdown filter due to type mismatch, orc type: {}, schema type: {}",
                    expect_nested_type->getName(),
                    nested_type->getName());
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            /// If null_as_default is true, the only difference is nullable, and the evaluations of current RPNElement based on default and null field
            /// have the same result, we still should push down current filter.
            if (format_settings.null_as_default && !column->type->isNullable() && !column->type->isLowCardinalityNullable())
            {
                bool match_if_null = evaluateRPNElement({}, curr);
                bool match_if_default = evaluateRPNElement(column->type->getDefault(), curr);
                if (match_if_default != match_if_null)
                {
                    builder.literal(orc::TruthValue::YES_NO_NULL);
                    break;
                }
            }

            auto predicate_type = convertORCTypeToPredicateType(*orc_type);
            if (!predicate_type.has_value())
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            if (need_wrap_not)
                builder.startNot();

            if (contains_is_null)
            {
                builder.isNull(orc_type->getColumnId(), *predicate_type);
            }
            else if (contains_in_range)
            {
                const auto & range = curr.range;
                bool has_left_bound = !range.left.isNegativeInfinity();
                bool has_right_bound = !range.right.isPositiveInfinity();
                if (!has_left_bound && !has_right_bound)
                {
                    /// Transform whole range orc::TruthValue::YES_NULL
                    builder.literal(orc::TruthValue::YES_NULL);
                }
                else if (has_left_bound && has_right_bound && range.left_included && range.right_included && range.left == range.right)
                {
                    /// Transform range with the same left bound and right bound to equal, which could utilize bloom filters in ORC
                    auto literal = convertFieldToORCLiteral(*orc_type, range.left);
                    if (literal.has_value())
                        builder.equals(orc_type->getColumnId(), *predicate_type, *literal);
                    else
                        builder.literal(orc::TruthValue::YES_NO_NULL);
                }
                else
                {
                    std::optional<orc::Literal> left_literal;
                    if (has_left_bound)
                        left_literal = convertFieldToORCLiteral(*orc_type, range.left);

                    std::optional<orc::Literal> right_literal;
                    if (has_right_bound)
                        right_literal = convertFieldToORCLiteral(*orc_type, range.right);

                    if (has_left_bound && has_right_bound)
                        builder.startAnd();

                    if (has_left_bound)
                    {
                        if (left_literal.has_value())
                        {
                            /// >= is transformed to not < and > is transformed to not <=
                            builder.startNot();
                            if (range.left_included)
                                builder.lessThan(orc_type->getColumnId(), *predicate_type, *left_literal);
                            else
                                builder.lessThanEquals(orc_type->getColumnId(), *predicate_type, *left_literal);
                            builder.end();
                        }
                        else
                            builder.literal(orc::TruthValue::YES_NO_NULL);
                    }

                    if (has_right_bound)
                    {
                        if (right_literal.has_value())
                        {
                            if (range.right_included)
                                builder.lessThanEquals(orc_type->getColumnId(), *predicate_type, *right_literal);
                            else
                                builder.lessThan(orc_type->getColumnId(), *predicate_type, *right_literal);
                        }
                        else
                            builder.literal(orc::TruthValue::YES_NO_NULL);
                    }

                    if (has_left_bound && has_right_bound)
                        builder.end();
                }
            }
            else if (contains_in_set)
            {
                /// Build literals from MergeTreeSetIndex
                const auto & ordered_set = set_index->getOrderedSet();
                const auto & set_column = ordered_set[0];

                bool fail = false;
                std::vector<orc::Literal> literals;
                literals.reserve(set_column->size());
                for (size_t i = 0; i < set_column->size(); ++i)
                {
                    auto literal = convertFieldToORCLiteral(*orc_type, (*set_column)[i]);
                    if (!literal.has_value())
                    {
                        fail = true;
                        break;
                    }

                    literals.emplace_back(*literal);
                }

                /// set has zero element
                if (literals.empty())
                    builder.literal(orc::TruthValue::YES);
                else if (fail)
                    builder.literal(orc::TruthValue::YES_NO_NULL);
                else
                    builder.in(orc_type->getColumnId(), *predicate_type, literals);
            }

            if (need_wrap_not)
                builder.end();

            break;
        }
        case KeyCondition::RPNElement::FUNCTION_UNKNOWN: {
            builder.literal(orc::TruthValue::YES_NO_NULL);
            rpn_stack.pop_back();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_NOT: {
            builder.startNot();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_AND: {
            builder.startAnd();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_OR: {
            builder.startOr();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::ALWAYS_FALSE: {
            builder.literal(orc::TruthValue::NO);
            rpn_stack.pop_back();
            break;
        }
        case KeyCondition::RPNElement::ALWAYS_TRUE: {
            builder.literal(orc::TruthValue::YES);
            rpn_stack.pop_back();
            break;
        }
    }
}

std::unique_ptr<orc::SearchArgument> buildORCSearchArgument(
    const KeyCondition & key_condition, const Block & header, const orc::Type & schema, const FormatSettings & format_settings)
{
    auto rpn_stack = key_condition.getRPN();
    if (rpn_stack.empty())
        return nullptr;

    auto builder = orc::SearchArgumentFactory::newBuilder();
    buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, *builder, format_settings);
    return builder->build();
}


IOMergeBuffer::IOMergeBuffer(std::shared_ptr<arrow::io::RandomAccessFile> random_file_, std::string filename_, size_t file_size_)
    : random_file(random_file_), filename(filename_), file_size(file_size_)
{
}

void IOMergeBuffer::SharedBuffer::align(int64_t align_size_, int64_t file_size_)
{
    if (align_size_ != 0)
    {
        offset = raw_offset / align_size_ * align_size_;
        int64_t end = std::min((raw_offset + raw_size + align_size_ - 1) / align_size_ * align_size_, file_size_);
        size = end - offset;
    }
    else
    {
        offset = raw_offset;
        size = raw_size;
    }
}
String IOMergeBuffer::SharedBuffer::toString()
{
    return fmt::format(
        " raw offset {} to {}, aligned offset {} to {}, refcount {}", raw_offset, raw_offset + raw_size, offset, offset + size, ref_count);
}

arrow::Status IOMergeBuffer::sortAndCheckOverlap(std::vector<IORange> & ranges)
{
    std::sort(ranges.begin(), ranges.end(), [](const IORange & a, const IORange & b) {
        if (a.offset != b.offset)
        {
            return a.offset < b.offset;
        }
        return a.size < b.size;
    });

    // check io range is not overlapped.
    for (size_t i = 1; i < ranges.size(); i++)
    {
        if (ranges[i].offset < (ranges[i - 1].offset + ranges[i - 1].size))
        {
            return arrow::Status::IOError("io ranges are overalpped");
        }
    }

    return arrow::Status::OK();
}
void IOMergeBuffer::mergeSmallRanges(const std::vector<IORange> & small_ranges)
{
    if (!small_ranges.empty())
    {
        auto update_map = [&](size_t from, size_t to) {
            // merge from [unmerge, i-1]
            int64_t ref_count = (to - from + 1);
            int64_t end = (small_ranges[to].offset + small_ranges[to].size);
            SharedBuffer sb = SharedBuffer{
                .raw_offset = small_ranges[from].offset, .raw_size = end - small_ranges[from].offset, .ref_count = ref_count};
            sb.align(align_size, file_size);
            buffer_map.insert(std::make_pair(sb.raw_offset + sb.raw_size, sb));
            // LOG_INFO(getLogger("updateMap"), " sb: {} ", sb.toString());
        };

        size_t unmerge = 0;
        for (size_t i = 1; i < small_ranges.size(); i++)
        {
            const auto & prev = small_ranges[i - 1];
            const auto & now = small_ranges[i];
            size_t now_end = now.offset + now.size;
            size_t prev_end = prev.offset + prev.size;
            if (((now_end - small_ranges[unmerge].offset) <= static_cast<size_t>(options.max_buffer_size))
                && (now.offset - prev_end) <= static_cast<size_t>(options.max_dist_size))
            {
                continue;
            }
            else
            {
                update_map(unmerge, i - 1);
                unmerge = i;
            }
        }
        update_map(unmerge, small_ranges.size() - 1);
    }
}

arrow::Status IOMergeBuffer::setIORanges(const std::vector<IORange> & ranges, [[maybe_unused]] bool coalesce_lazy_column)
{
    if (ranges.size() == 0)
    {
        return arrow::Status::OK();
    }

    std::vector<IORange> check(ranges);
    if (auto st = sortAndCheckOverlap(check); !st.ok())
    {
        return st;
    }


    std::vector<IORange> small_ranges;
    for (const IORange & r : check)
    {
        if (r.size > options.max_buffer_size)
        {
            SharedBuffer sb = SharedBuffer{.raw_offset = r.offset, .raw_size = r.size, .ref_count = 1};
            sb.align(align_size, file_size);
            buffer_map.insert(std::make_pair(sb.raw_offset + sb.raw_size, sb));
        }
        else
        {
            small_ranges.emplace_back(r);
        }
    }

    mergeSmallRanges(small_ranges);
    return arrow::Status::OK();
}

void IOMergeBuffer::release()
{
    buffer_map.clear();
}

arrow::Result<IOMergeBuffer::SharedBuffer *> IOMergeBuffer::findSharedBuffer(size_t offset, size_t count)
{
    auto iter = buffer_map.upper_bound(offset);
    if (iter == buffer_map.end())
    {
        return arrow::Status::IOError("failed to find shared buffer based on offset");
    }
    SharedBuffer & sb = iter->second;
    if ((static_cast<size_t>(sb.offset) > offset) || static_cast<size_t>(sb.offset + sb.size) < (offset + count))
    {
        return arrow::Status::IOError("bad construction of shared buffer");
    }
    return &sb;
}

arrow::Status IOMergeBuffer::readAtFully(int64_t offset, void * out, int64_t count)
{
    auto ret = findSharedBuffer(offset, count);
    if (!ret.ok())
    {
        auto st = random_file->ReadAt(offset, count, out);
        // LOG_INFO(getLogger("readAtFully - Direct"), "read from {} to {}", offset, offset + count);
        if (!st.ok())
            return st.status();
        ProfileEvents::increment(ProfileEvents::OrcIODirectCount, 1);
        ProfileEvents::increment(ProfileEvents::OrcIODirectBytes, count);
        return arrow::Status::OK();
    }
    SharedBuffer & sb = *ret.ValueOrDie();
    if (sb.buffer.capacity() == 0)
    {
        sb.buffer.reserve(sb.size);
        auto st = random_file->ReadAt(sb.offset, sb.size, sb.buffer.data());
        // LOG_INFO(getLogger("readAtFully - Shared"), "read from {} to {}", sb.offset, sb.offset + sb.size);
        if (!st.ok())
            return st.status();
        ProfileEvents::increment(ProfileEvents::OrcIOMergedCount, 1);
        ProfileEvents::increment(ProfileEvents::OrcIOMergedBytes, sb.size);
    }
    // LOG_INFO(getLogger("readAtFully"), "read offset {}, to {} buffer offset {}, to {} ", offset, offset + count,  sb.offset, sb.offset + sb.buffer.capacity());
    uint8_t * buffer = sb.buffer.data() + offset - sb.offset;
    std::memcpy(out, buffer, count);
    ProfileEvents::increment(ProfileEvents::OrcIOSharedCount, 1);
    ProfileEvents::increment(ProfileEvents::OrcIOSharedBytes, count);

    return arrow::Status::OK();
}


// TODO(renming): fix name here.
CachedORCArrowInputStream::CachedORCArrowInputStream(const std::shared_ptr<arrow::io::RandomAccessFile> & file_) : file(file_)
{
    merge_buffer = std::make_unique<IOMergeBuffer>(file_, getName(), file_->GetSize().ValueOrDie());
}

uint64_t CachedORCArrowInputStream::getLength() const
{
    return file->GetSize().ValueOrDie();
}

uint64_t CachedORCArrowInputStream::getNaturalReadSize() const
{
    // return 128 * 1024;
    return 8 * 1024 * 1024;
}

uint64_t CachedORCArrowInputStream::getNaturalReadSizeAfterSeek() const
{
    // return 128 * 1024;
    return 2 * 1024 * 1024;
}

void CachedORCArrowInputStream::prepareCache(PrepareCacheScope scope, uint64_t offset, uint64_t length)
{
    static size_t orc_file_cache_max_size = 8 * 1024 * 1024;
    static size_t orc_row_index_cache_max_size = 1024 * 1024;
    static size_t orc_stripe_cache_max_size = 8 * 1024 * 1024;
    size_t max_cache_size = 0;
    switch (scope)
    {
        case PrepareCacheScope::READ_FULL_FILE:
            max_cache_size = orc_file_cache_max_size;
            break;
        case PrepareCacheScope::READ_FULL_ROW_INDEX:
            max_cache_size = orc_row_index_cache_max_size;
            break;
        case PrepareCacheScope::READ_FULL_STRIPE:
            max_cache_size = orc_stripe_cache_max_size;
            break;
    }
    if (length > max_cache_size)
        return;
    if (isAlreadyCachedInBuffer(offset, length))
        return;
    cached_buffer.resize(length);
    cached_offset = offset;

    std::vector<IORange> io_ranges{};
    io_ranges.emplace_back(orc::InputStream::IORange{.offset = offset, .size = length, .is_active = true});
    setIORanges(io_ranges);

    readDirect(cached_buffer.data(), length, offset);
}

bool CachedORCArrowInputStream::isAlreadyCachedInBuffer(uint64_t offset, uint64_t length)
{
    return !cached_buffer.empty() && offset >= cached_offset && (offset + length) < (cached_offset + cached_buffer.size());
}


void CachedORCArrowInputStream::readDirect(void * buf, uint64_t length, uint64_t offset)
{
    auto st = merge_buffer->readAtFully(offset, buf, length);
    if (!st.ok())
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "read failed at {}", offset);
    }

    ProfileEvents::increment(ProfileEvents::OrcReadDirectCount, 1);

    // if (static_cast<uint64_t>(bytes_read) != length)
    // {
    //     throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Short read from arrow input file");
    // }
}

void CachedORCArrowInputStream::read(void * buf, uint64_t length, uint64_t offset)
{
    if (isAlreadyCachedInBuffer(offset, length))
    {
        size_t idx = offset - cached_offset;
        memcpy(buf, cached_buffer.data() + idx, length);
        ProfileEvents::increment(ProfileEvents::OrcReadCacheCount, 1);
    }
    else
    {
        readDirect(buf, length, offset);
    }
}

void CachedORCArrowInputStream::setIORanges(std::vector<orc::InputStream::IORange> & io_ranges)
{
    if (!merge_buffer)
        return;

    std::vector<IOMergeBuffer::IORange> bs_io_ranges;
    bs_io_ranges.reserve(io_ranges.size());
    for (const auto & r : io_ranges)
    {
        bs_io_ranges.emplace_back(static_cast<int64_t>(r.offset), static_cast<int64_t>(r.size), r.is_active);
    }


    arrow::Status st = merge_buffer->setIORanges(bs_io_ranges, true);

    if (!st.ok())
    {
        throw DB::Exception(ErrorCodes::INCORRECT_DATA, "Failed to setIORanges {}", st.ToString());
    }
}

void CachedORCArrowInputStream::clearIORanges()
{
    if (!merge_buffer)
        return;
    merge_buffer->release();
}

const std::string & CachedORCArrowInputStream::getName() const
{
    static const std::string filename("ArrowInputFile");
    return filename;
}

ORCColumnToCHColumn::ORCColumnToCHColumn(
    const Block & header_, bool allow_missing_columns_, bool null_as_default_, bool case_insensitive_matching_, bool allow_out_of_range_)
    : header(header_)
    , allow_missing_columns(allow_missing_columns_)
    , null_as_default(null_as_default_)
    , case_insensitive_matching(case_insensitive_matching_)
    , allow_out_of_range(allow_out_of_range_)
{
    header_columns = header.getNameSet();
}

void ORCColumnToCHColumn::orcTableToCHChunk(
    Chunk & res,
    const orc::Type * schema,
    const orc::ColumnVectorBatch * table,
    size_t num_rows,
    BlockMissingValues * block_missing_values) const
{
    const orc::StructVectorBatch * struct_batch = dynamic_cast<const orc::StructVectorBatch *>(table);
    if (!struct_batch)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table must be StructVectorBatch but is {}", struct_batch->toString());

    if (schema->getSubtypeCount() != struct_batch->fields.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ORC ColumnVectorBatch has {} fields but schema has {}",
            struct_batch->fields.size(),
            schema->getSubtypeCount());

    size_t field_num = struct_batch->fields.size();
    NameToColumnPtr name_to_column_ptr;
    for (size_t i = 0; i < field_num; ++i)
    {
        auto name = schema->getFieldName(i);

        // the schema might contain lazy columns which is not set in header.
        if (!header_columns.contains(name))
            continue;

        auto * field = struct_batch->fields[i];
        if (!field)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table field {} is null", name);

        if (case_insensitive_matching)
            boost::to_lower(name);
        field->numElements = num_rows;
        name_to_column_ptr[std::move(name)] = {field, schema->getSubtype(i)};
    }
    // LOG_INFO(logger, "active input batch {} ", table->toString());
    orcColumnsToCHChunk(res, name_to_column_ptr, num_rows, block_missing_values);
}

ORCArrowInputStream::ORCArrowInputStream(const std::shared_ptr<arrow::io::RandomAccessFile> & file_) : file(file_)
{
}

uint64_t ORCArrowInputStream::getLength() const
{
    return file->GetSize().ValueOrDie();
}

uint64_t ORCArrowInputStream::getNaturalReadSize() const
{
    // return 128 * 1024;
    return 1024 * 1024;
}

void ORCArrowInputStream::read(void * buf, uint64_t length, uint64_t offset)
{
    int64_t bytes_read = file->ReadAt(offset, length, buf).ValueOrDie();

    if (static_cast<uint64_t>(bytes_read) != length)
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Short read from arrow input file");
    }
}

const std::string & ORCArrowInputStream::getName() const
{
    static const std::string filename("ArrowInputFile");
    return filename;
}

void ORCColumnToCHColumn::orcTableToCHChunkWithFields(
    const Block & local_header,
    Chunk & res,
    const orc::Type * schema,
    const orc::ColumnVectorBatch * table,
    const std::vector<int> & fields,
    size_t num_rows,
    BlockMissingValues * block_missing_values) const
{
    const orc::StructVectorBatch * struct_batch = dynamic_cast<const orc::StructVectorBatch *>(table);
    if (!struct_batch)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table must be StructVectorBatch but is {}", struct_batch->toString());

    if (schema->getSubtypeCount() != struct_batch->fields.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ORC ColumnVectorBatch has {} fields but schema has {}",
            struct_batch->fields.size(),
            schema->getSubtypeCount());

    size_t field_num = fields.size();
    NameToColumnPtr name_to_column_ptr;
    for (size_t i = 0; i < field_num; ++i)
    {
        auto pos = fields[i];
        auto name = schema->getFieldName(pos);

        auto * field = struct_batch->fields[pos];
        if (!field)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table field {} is null", name);

        if (case_insensitive_matching)
            boost::to_lower(name);
        field->numElements = num_rows;
        name_to_column_ptr[std::move(name)] = {field, schema->getSubtype(pos)};
    }
    // LOG_INFO(logger, "lazy input batch {} ", table->toString());
    orcColumnsToCHChunk(local_header, res, name_to_column_ptr, num_rows, block_missing_values);
}

// void ORCColumnToCHColumn::orcTableToCHChunkStage2(
//     Chunk & res,
//     const orc::Type * schema,
//     const orc::ColumnVectorBatch * table,
//     const std::vector<size_t> & fields,
//     size_t num_rows,
//     BlockMissingValues * block_missing_values) const
// {
//     const orc::StructVectorBatch * struct_batch = dynamic_cast<const orc::StructVectorBatch *>(table);
//     if (!struct_batch)
//         throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table must be StructVectorBatch but is {}", struct_batch->toString());

//     if (schema->getSubtypeCount() != struct_batch->fields.size())
//         throw Exception(
//             ErrorCodes::LOGICAL_ERROR,
//             "ORC ColumnVectorBatch has {} fields but schema has {}",
//             struct_batch->fields.size(),
//             schema->getSubtypeCount());

//     size_t field_num = schema->getSubtypeCount();
//     NameToColumnPtr name_to_column_ptr;
//     for (size_t i = 0; i < field_num; ++i)
//     {
//         auto name = schema->getFieldName(i);

//         auto * field = struct_batch->fields[i];
//         if (!field)
//             throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table field {} is null", name);

//         if (case_insensitive_matching)
//             boost::to_lower(name);
//         field->numElements = num_rows;
//         name_to_column_ptr[std::move(name)] = {field, schema->getSubtype(i)};
//     }
//     LOG_INFO(logger, "lazy input batch {} ", table->toString());
//     orcColumnsToCHChunk(res, name_to_column_ptr, num_rows, block_missing_values);
// }

/// Creates a null bytemap from ORC's not-null bytemap
static ColumnPtr readByteMapFromORCColumn(const orc::ColumnVectorBatch * orc_column)
{
    if (!orc_column->hasNulls)
        return ColumnUInt8::create(orc_column->numElements, 0);

    auto nullmap_column = ColumnUInt8::create();
    PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(*nullmap_column).getData();
    bytemap_data.resize(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        bytemap_data[i] = 1 - orc_column->notNull[i];
    return nullmap_column;
}


static const orc::ColumnVectorBatch * getNestedORCColumn(const orc::ListVectorBatch * orc_column)
{
    return orc_column->elements.get();
}

template <typename BatchType>
static ColumnPtr readOffsetsFromORCListColumn(const BatchType * orc_column)
{
    auto offsets_column = ColumnUInt64::create();
    ColumnArray::Offsets & offsets_data = assert_cast<ColumnVector<UInt64> &>(*offsets_column).getData();
    offsets_data.reserve(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        offsets_data.push_back(orc_column->offsets[i + 1]);

    return offsets_column;
}

static ColumnWithTypeAndName
readColumnWithBooleanData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_bool_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_type = DataTypeFactory::instance().get("Bool");
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt8> &>(*internal_column).getData();
    column_data.reserve(orc_bool_column->numElements);

    for (size_t i = 0; i < orc_bool_column->numElements; ++i)
        column_data.push_back(static_cast<UInt8>(orc_bool_column->data[i]));

    return {std::move(internal_column), internal_type, column_name};
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType, typename BatchType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName
readColumnWithNumericData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);
    column_data.insert_assume_reserved(orc_int_column->data.data(), orc_int_column->data.data() + orc_int_column->numElements);

    return {std::move(internal_column), std::move(internal_type), column_name};
}

template <typename NumericType, typename BatchType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName
readColumnWithNumericDataCast(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_int_column->numElements; ++i)
        column_data.push_back(static_cast<NumericType>(orc_int_column->data[i]));

    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName
readColumnWithStringData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeString>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    size_t reserver_size = 0;
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
            reserver_size += orc_str_column->length[i];
        reserver_size += 1;
    }

    column_chars_t.reserve(reserver_size);
    column_offsets.reserve(orc_str_column->numElements);

    size_t curr_offset = 0;
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
        {
            const auto * buf = orc_str_column->data[i];
            size_t buf_size = orc_str_column->length[i];
            column_chars_t.insert_assume_reserved(buf, buf + buf_size);
            curr_offset += buf_size;
        }

        column_chars_t.push_back(0);
        ++curr_offset;

        column_offsets.push_back(curr_offset);
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}


static ColumnWithTypeAndName
readColumnWithFixedStringData(const orc::ColumnVectorBatch * orc_column, const orc::Type * orc_type, const String & column_name)
{
    size_t fixed_len = orc_type->getMaximumLength();
    auto internal_type = std::make_shared<DataTypeFixedString>(fixed_len);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnFixedString &>(*internal_column).getChars();
    column_chars_t.reserve(orc_column->numElements * fixed_len);

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
            column_chars_t.insert_assume_reserved(orc_str_column->data[i], orc_str_column->data[i] + orc_str_column->length[i]);
        else
            column_chars_t.resize_fill(column_chars_t.size() + fixed_len);
    }

    return {std::move(internal_column), std::move(internal_type), column_name};
}


template <typename DecimalType, typename BatchType, typename VectorType = ColumnDecimal<DecimalType>>
static ColumnWithTypeAndName readColumnWithDecimalDataCast(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, DataTypePtr internal_type)
{
    using NativeType = typename DecimalType::NativeType;
    static_assert(std::is_same_v<BatchType, orc::Decimal128VectorBatch> || std::is_same_v<BatchType, orc::Decimal64VectorBatch>);

    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_decimal_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_decimal_column->numElements; ++i)
    {
        DecimalType decimal_value;
        if constexpr (std::is_same_v<BatchType, orc::Decimal128VectorBatch>)
        {
            Int128 int128_value;
            int128_value.items[0] = orc_decimal_column->values[i].getLowBits();
            int128_value.items[1] = orc_decimal_column->values[i].getHighBits();
            decimal_value.value = static_cast<NativeType>(int128_value);
        }
        else
            decimal_value.value = static_cast<NativeType>(orc_decimal_column->values[i]);

        column_data.push_back(std::move(decimal_value));
    }

    return {std::move(internal_column), internal_type, column_name};
}

template <typename ColumnType>
static ColumnWithTypeAndName readColumnWithBigNumberFromBinaryData(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, const DataTypePtr & column_type)
{
    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);

    auto internal_column = column_type->createColumn();
    auto & integer_column = assert_cast<ColumnType &>(*internal_column);
    integer_column.reserve(orc_str_column->numElements);

    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
        {
            if (sizeof(typename ColumnType::ValueType) != orc_str_column->length[i])
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "ValueType size {} of column {} is not equal to size of binary data {}",
                    sizeof(typename ColumnType::ValueType),
                    integer_column.getName(),
                    orc_str_column->length[i]);

            integer_column.insertData(orc_str_column->data[i], orc_str_column->length[i]);
        }
        else
        {
            integer_column.insertDefault();
        }
    }
    return {std::move(internal_column), column_type, column_name};
}

static ColumnWithTypeAndName readColumnWithDateData(
    const orc::ColumnVectorBatch * orc_column,
    const orc::Type *,
    const String & column_name,
    const bool allow_out_of_range,
    const DataTypePtr & type_hint)
{
    DataTypePtr internal_type;
    bool check_date_range = false;
    /// Make result type Date32 when requested type is actually Date32 or when we use schema inference
    if (!type_hint || (type_hint && isDate32(*type_hint)))
    {
        internal_type = std::make_shared<DataTypeDate32>();
        check_date_range = true;
    }
    else
    {
        internal_type = std::make_shared<DataTypeInt32>();
    }

    const auto * orc_int_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<Int32> & column_data = assert_cast<ColumnVector<Int32> &>(*internal_column).getData();
    column_data.reserve(orc_int_column->numElements);

    for (size_t i = 0; i < orc_int_column->numElements; ++i)
    {
        Int32 days_num = static_cast<Int32>(orc_int_column->data[i]);
        if (check_date_range && (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM || days_num < -DAYNUM_OFFSET_EPOCH))
        {
            if (!allow_out_of_range)
                throw Exception{
                    fmt::format(
                        "Input value {} of a column \"{}\" is greater than max allowed Date value, which is {}",
                        days_num,
                        internal_column->getName(),
                        DATE_LUT_MAX_DAY_NUM),
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE};

            if (days_num < -DAYNUM_OFFSET_EPOCH)
                days_num = -DAYNUM_OFFSET_EPOCH;
            else if (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM)
                days_num = DATE_LUT_MAX_EXTEND_DAY_NUM;
        }

        column_data.push_back(days_num);
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readColumnWithTimestampData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_ts_column = dynamic_cast<const orc::TimestampVectorBatch *>(orc_column);

    auto internal_type = std::make_shared<DataTypeDateTime64>(9);
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnDateTime64 &>(*internal_column).getData();
    column_data.reserve(orc_ts_column->numElements);

    constexpr Int64 multiplier = 1e9L;
    for (size_t i = 0; i < orc_ts_column->numElements; ++i)
    {
        Decimal64 decimal64;
        decimal64.value = orc_ts_column->data[i] * multiplier + orc_ts_column->nanoseconds[i];
        column_data.emplace_back(std::move(decimal64));
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}


ColumnPtr sdictToStringColumn(orc::StringDictionary & dict)
{
    auto str_type = std::make_shared<DataTypeString>();
    auto internal_column = str_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();
    auto orc_blob = dict.dictionaryBlob.data();
    auto orc_num = dict.dictionaryOffset.size() - 1;
    auto orc_blob_size = dict.dictionaryBlob.size();
    column_chars_t.reserve(orc_blob_size + orc_num);
    column_offsets.reserve(orc_num);
    size_t curr_offset = 0;
    for (size_t i = 0; i < orc_num; i++)
    {
        auto len = dict.dictionaryOffset[i + 1] - dict.dictionaryOffset[i];
        column_chars_t.insert_assume_reserved(orc_blob, orc_blob + len);
        column_chars_t.push_back(0);
        curr_offset += 1 + len;
        orc_blob = orc_blob + len;
        column_offsets.push_back(curr_offset);
    }
    return internal_column;
}

static ColumnWithTypeAndName readColumnFromORCColumn(
    const orc::ColumnVectorBatch * orc_column,
    const orc::Type * orc_type,
    const std::string & column_name,
    bool inside_nullable,
    bool allow_out_of_range,
    DataTypePtr type_hint = nullptr)
{
    bool skipped = false;

    // special case for lowcard
    if (auto * orc_str_batch = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
        orc_str_batch && type_hint && (type_hint->isLowCardinalityNullable() || type_hint->lowCardinality()))
    {
        auto inner = removeLowCardinality(type_hint);
        if ((inner->isNullable() && dynamic_cast<const DataTypeNullable *>(inner.get())->getNestedType()->getTypeId() == TypeIndex::String)
            || inner->getTypeId() == TypeIndex::String)
        {
            // stored as dict column
            bool has_nulls = orc_column->hasNulls;
            auto internal_column = type_hint->createColumn();
            ColumnLowCardinality & column_lc = assert_cast<ColumnLowCardinality &>(*internal_column);


            DataTypeUInt64 indexes_type;
            auto indexes_column = indexes_type.createColumn();
            if (orc_str_batch->use_codes && orc_str_batch->use_type_hint)
            { // dict encoded.
                // read dict
                auto dict_column = IColumn::mutate(column_lc.getDictionaryPtr());
                auto * uniq_column = static_cast<IColumnUnique *>(dict_column.get());
                auto values_column = sdictToStringColumn(*orc_str_batch->dictionary.get());
                uniq_column->uniqueInsertRangeFrom(*values_column, 0, values_column->size());

                // read codes
                auto & column_data = static_cast<ColumnVector<UInt64> &>(*indexes_column).getData();
                const auto & codes = orc_str_batch->codes;
                column_data.reserve(codes.size());

                column_data.insert_assume_reserved(codes.data(), codes.data() + orc_column->numElements);
                auto lc = ColumnLowCardinality::create(std::move(dict_column), std::move(indexes_column));
                LOG_TRACE(getLogger(__FUNCTION__), "read lc from dict page with structure {}", lc->dumpStructure());
                return {std::move(lc), type_hint, column_name};
            }
            else if (orc_str_batch->use_codes)
            {
                auto dict_column = IColumn::mutate(column_lc.getDictionaryPtr());
                auto * uniq_column = static_cast<IColumnUnique *>(dict_column.get());
                auto values_column = sdictToStringColumn(*orc_str_batch->dictionary.get());
                uniq_column->uniqueInsertRangeFrom(*values_column, 0, values_column->size());
                // read codes
                auto & column_data = static_cast<ColumnVector<UInt64> &>(*indexes_column).getData();
                const auto & codes = orc_str_batch->codes;
                column_data.reserve(codes.size());
                if (!has_nulls)
                {
                    for (size_t i = 0; i < orc_column->numElements; i++)
                    {
                        column_data.push_back(codes[i] + 2);
                    }
                }
                else
                {
                    for (size_t i = 0; i < orc_column->numElements; i++)
                    {
                        if (orc_column->notNull[i])
                        {
                            column_data.push_back(codes[i] + 2);
                        }
                        else
                        {
                            column_data.push_back(0);
                        }
                    }
                }
                auto lc = ColumnLowCardinality::create(std::move(dict_column), std::move(indexes_column));
                LOG_TRACE(getLogger(__FUNCTION__), "read lc from dict page with structure {}", lc->dumpStructure());
                return {std::move(lc), type_hint, column_name};
            }
            else
            {
                // direct.
                auto full_string_col
                    = readColumnFromORCColumn(orc_column, orc_type, column_name, inside_nullable, allow_out_of_range, inner);
                auto dict_column = column_lc.getDictionaryPtr()->cloneEmpty();
                auto lc
                    = ColumnLowCardinality::create(std::move(dict_column), std::move(indexes_column), std::move(full_string_col.column));
                LOG_TRACE(getLogger(__FUNCTION__), "read lc from direct page with structure {}", lc->dumpStructure());
                return {std::move(lc), type_hint, column_name};
            }
        }
    }

    if (!inside_nullable && (orc_column->hasNulls || (type_hint && type_hint->isNullable()))
        && (orc_type->getKind() != orc::LIST && orc_type->getKind() != orc::MAP && orc_type->getKind() != orc::STRUCT))
    {
        DataTypePtr nested_type_hint;
        if (type_hint)
            nested_type_hint = removeNullable(type_hint);

        auto nested_column = readColumnFromORCColumn(orc_column, orc_type, column_name, true, allow_out_of_range, nested_type_hint);

        auto nullmap_column = readByteMapFromORCColumn(orc_column);
        auto nullable_type = std::make_shared<DataTypeNullable>(std::move(nested_column.type));
        auto nullable_column = ColumnNullable::create(nested_column.column, nullmap_column);
        return {std::move(nullable_column), std::move(nullable_type), column_name};
    }

    switch (orc_type->getKind())
    {
        case orc::STRING:
        case orc::BINARY:
        case orc::VARCHAR: {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    // case TypeIndex::IPv6:
                    //     return readIPv6ColumnFromBinaryData(orc_column, orc_type, column_name);
                    /// ORC format outputs big integers as binary column, because there is no fixed binary in ORC.
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, orc_type, column_name, type_hint);
                    /// ORC doesn't support Decimal256 as separate type. We read and write it as binary data.
                    case TypeIndex::Decimal256:
                        return readColumnWithBigNumberFromBinaryData<ColumnDecimal<Decimal256>>(
                            orc_column, orc_type, column_name, type_hint);
                    default:;
                }
            }
            return readColumnWithStringData(orc_column, orc_type, column_name);
        }
        case orc::CHAR: {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, orc_type, column_name, type_hint);
                    default:;
                }
            }
            return readColumnWithFixedStringData(orc_column, orc_type, column_name);
        }
        case orc::BOOLEAN:
            return readColumnWithBooleanData(orc_column, orc_type, column_name);
        case orc::BYTE:
            return readColumnWithNumericDataCast<Int8, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::SHORT:
            return readColumnWithNumericDataCast<Int16, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::INT: {
            /// ORC format doesn't have unsigned integers and we output IPv4 as Int32.
            /// We should allow to read it back from Int32.
            // if (type_hint && isIPv4(type_hint))
            //     return readIPv4ColumnWithInt32Data(orc_column, orc_type, column_name);
            return readColumnWithNumericDataCast<Int32, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        }
        case orc::LONG:
            return readColumnWithNumericData<Int64, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::FLOAT:
            return readColumnWithNumericDataCast<Float32, orc::DoubleVectorBatch>(orc_column, orc_type, column_name);
        case orc::DOUBLE:
            return readColumnWithNumericData<Float64, orc::DoubleVectorBatch>(orc_column, orc_type, column_name);
        case orc::DATE:
            return readColumnWithDateData(orc_column, orc_type, column_name, allow_out_of_range, type_hint);
        case orc::TIMESTAMP:
            return readColumnWithTimestampData(orc_column, orc_type, column_name);
        case orc::DECIMAL: {
            auto interal_type = parseORCType(orc_type, false, skipped);

            auto precision = orc_type->getPrecision();
            if (precision == 0)
                precision = 38;

            if (precision <= DecimalUtils::max_precision<Decimal32>)
                return readColumnWithDecimalDataCast<Decimal32, orc::Decimal64VectorBatch>(orc_column, orc_type, column_name, interal_type);
            else if (precision <= DecimalUtils::max_precision<Decimal64>)
                return readColumnWithDecimalDataCast<Decimal64, orc::Decimal64VectorBatch>(orc_column, orc_type, column_name, interal_type);
            else if (precision <= DecimalUtils::max_precision<Decimal128>)
                return readColumnWithDecimalDataCast<Decimal128, orc::Decimal128VectorBatch>(
                    orc_column, orc_type, column_name, interal_type);
            else
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Decimal precision {} in ORC type {} is out of bound",
                    precision,
                    orc_type->toString());
        }
        case orc::MAP: {
            DataTypePtr key_type_hint;
            DataTypePtr value_type_hint;
            if (type_hint)
            {
                const auto * map_type_hint = typeid_cast<const DataTypeMap *>(type_hint.get());
                if (map_type_hint)
                {
                    key_type_hint = map_type_hint->getKeyType();
                    value_type_hint = map_type_hint->getValueType();
                }
            }

            const auto * orc_map_column = dynamic_cast<const orc::MapVectorBatch *>(orc_column);
            const auto * orc_key_column = orc_map_column->keys.get();
            const auto * orc_value_column = orc_map_column->elements.get();
            const auto * orc_key_type = orc_type->getSubtype(0);
            const auto * orc_value_type = orc_type->getSubtype(1);

            auto key_column = readColumnFromORCColumn(orc_key_column, orc_key_type, "key", false, allow_out_of_range, key_type_hint);
            if (key_type_hint && !key_type_hint->equals(*key_column.type))
            {
                /// Cast key column to target type, because it can happen
                /// that parsed type cannot be ClickHouse Map key type.
                key_column.column = castColumn(key_column, key_type_hint);
                key_column.type = key_type_hint;
            }

            auto value_column
                = readColumnFromORCColumn(orc_value_column, orc_value_type, "value", false, allow_out_of_range, value_type_hint);
            if (skipped)
                return {};

            auto offsets_column = readOffsetsFromORCListColumn(orc_map_column);
            auto map_column = ColumnMap::create(key_column.column, value_column.column, offsets_column);
            auto map_type = std::make_shared<DataTypeMap>(key_column.type, value_column.type);
            return {std::move(map_column), std::move(map_type), column_name};
        }
        case orc::LIST: {
            DataTypePtr nested_type_hint;
            if (type_hint)
            {
                const auto * array_type_hint = typeid_cast<const DataTypeArray *>(type_hint.get());
                if (array_type_hint)
                    nested_type_hint = array_type_hint->getNestedType();
            }

            const auto * orc_list_column = dynamic_cast<const orc::ListVectorBatch *>(orc_column);
            const auto * orc_nested_column = getNestedORCColumn(orc_list_column);
            const auto * orc_nested_type = orc_type->getSubtype(0);
            auto nested_column
                = readColumnFromORCColumn(orc_nested_column, orc_nested_type, column_name, false, allow_out_of_range, nested_type_hint);

            auto offsets_column = readOffsetsFromORCListColumn(orc_list_column);
            auto array_column = ColumnArray::create(nested_column.column, offsets_column);
            auto array_type = std::make_shared<DataTypeArray>(nested_column.type);
            return {std::move(array_column), std::move(array_type), column_name};
        }
        case orc::STRUCT: {
            Columns tuple_elements;
            DataTypes tuple_types;
            std::vector<String> tuple_names;
            const auto * tuple_type_hint = type_hint ? typeid_cast<const DataTypeTuple *>(type_hint.get()) : nullptr;

            const auto * orc_struct_column = dynamic_cast<const orc::StructVectorBatch *>(orc_column);
            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                const auto & field_name = orc_type->getFieldName(i);

                DataTypePtr nested_type_hint;
                if (tuple_type_hint)
                {
                    if (tuple_type_hint->haveExplicitNames())
                    {
                        auto pos = tuple_type_hint->tryGetPositionByName(field_name);
                        if (pos)
                            nested_type_hint = tuple_type_hint->getElement(*pos);
                    }
                    else if (size_t(i) < tuple_type_hint->getElements().size())
                        nested_type_hint = tuple_type_hint->getElement(i);
                }

                const auto * nested_orc_column = orc_struct_column->fields[i];
                const auto * nested_orc_type = orc_type->getSubtype(i);
                auto element
                    = readColumnFromORCColumn(nested_orc_column, nested_orc_type, field_name, false, allow_out_of_range, nested_type_hint);

                tuple_elements.emplace_back(std::move(element.column));
                tuple_types.emplace_back(std::move(element.type));
                tuple_names.emplace_back(std::move(element.name));
            }

            auto tuple_column = ColumnTuple::create(std::move(tuple_elements));
            auto tuple_type = std::make_shared<DataTypeTuple>(std::move(tuple_types), std::move(tuple_names));
            return {std::move(tuple_column), std::move(tuple_type), column_name};
        }
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "Unsupported ORC type {} while reading column {}.", orc_type->toString(), column_name);
    }
}
void ORCColumnToCHColumn::orcColumnsToCHChunk(
    const Block & local_header,
    Chunk & res,
    NameToColumnPtr & name_to_column_ptr,
    size_t num_rows,
    BlockMissingValues * block_missing_values) const
{
    Columns columns_list;
    columns_list.reserve(local_header.columns());
    std::unordered_map<String, std::pair<BlockPtr, std::shared_ptr<NestedColumnExtractHelper>>> nested_tables;
    for (size_t column_i = 0, columns = local_header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = local_header.getByPosition(column_i);

        auto search_column_name = header_column.name;
        if (case_insensitive_matching)
            boost::to_lower(search_column_name);

        ColumnWithTypeAndName column;
        if (!name_to_column_ptr.contains(search_column_name))
        {
            bool read_from_nested = false;

            /// Check if it's a column from nested table.
            String nested_table_name = Nested::extractTableName(header_column.name);
            String search_nested_table_name = nested_table_name;
            if (case_insensitive_matching)
                boost::to_lower(search_nested_table_name);
            if (name_to_column_ptr.contains(search_nested_table_name))
            {
                if (!nested_tables.contains(search_nested_table_name))
                {
                    NamesAndTypesList nested_columns;
                    for (const auto & name_and_type : local_header.getNamesAndTypesList())
                    {
                        if (name_and_type.name.starts_with(nested_table_name + "."))
                            nested_columns.push_back(name_and_type);
                    }
                    auto nested_table_type = Nested::collect(nested_columns).front().type;

                    auto orc_column_with_type = name_to_column_ptr[search_nested_table_name];
                    ColumnsWithTypeAndName cols = {readColumnFromORCColumn(
                        orc_column_with_type.first,
                        orc_column_with_type.second,
                        nested_table_name,
                        false,
                        allow_out_of_range,
                        nested_table_type)};
                    BlockPtr block_ptr = std::make_shared<Block>(cols);
                    auto column_extractor = std::make_shared<NestedColumnExtractHelper>(*block_ptr, case_insensitive_matching);
                    nested_tables[search_nested_table_name] = {block_ptr, column_extractor};
                }

                auto nested_column = nested_tables[search_nested_table_name].second->extractColumn(search_column_name);
                if (nested_column)
                {
                    column = *nested_column;
                    if (case_insensitive_matching)
                        column.name = header_column.name;
                    read_from_nested = true;
                }
            }

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};
                else
                {
                    column.name = header_column.name;
                    column.type = header_column.type;
                    column.column = header_column.column->cloneResized(num_rows);
                    columns_list.push_back(std::move(column.column));
                    if (block_missing_values)
                        block_missing_values->setBits(column_i, num_rows);
                    continue;
                }
            }
        }
        else
        {
            auto orc_column_with_type = name_to_column_ptr[search_column_name];
            // if (auto * orc_str_batch = dynamic_cast<const orc::StringVectorBatch *>(orc_column_with_type.first); orc_str_batch)
            // {
            //     LOG_INFO(
            //         getLogger(__PRETTY_FUNCTION__),
            //         "before orc batch {} {}  dict {}  use codes {}",
            //         orc_column_with_type.first->numElements,
            //         orc_column_with_type.second->toString(),
            //         orc_str_batch->dictionary.get() == nullptr,
            //         orc_str_batch->use_codes);
            // }
            column = readColumnFromORCColumn(
                orc_column_with_type.first, orc_column_with_type.second, header_column.name, false, allow_out_of_range, header_column.type);
            // LOG_INFO(
            //     getLogger(__PRETTY_FUNCTION__),
            //     "orc batch {} {}, ce {}",
            //     orc_column_with_type.first->numElements,
            //     orc_column_with_type.second->toString(),
            //     column.dumpStructure());
        }

        if (null_as_default)
            insertNullAsDefaultIfNeeded(column, header_column, column_i, block_missing_values);

        try
        {
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while converting column {} from type {} to type {}",
                header_column.name,
                column.type->getName(),
                header_column.type->getName()));
            throw;
        }

        column.type = header_column.type;
        columns_list.push_back(std::move(column.column));
    }

    res.setColumns(columns_list, num_rows);
}
void ORCColumnToCHColumn::orcColumnsToCHChunk(
    Chunk & res, NameToColumnPtr & name_to_column_ptr, size_t num_rows, BlockMissingValues * block_missing_values) const
{
    orcColumnsToCHChunk(header, res, name_to_column_ptr, num_rows, block_missing_values);
}

IStorage::ColumnSizeByName getOrcColumnsSize(orc::Reader & orc_reader)
{
    auto stripe_info_ptr = orc_reader.getStripe(0);
    const auto & root_type = orc_reader.getType();

    if (!stripe_info_ptr)
    {
        LOG_INFO(getLogger("ORCBlockInputFormat"), "cannot get columns size, stripe info ptr is nullptr.");
        return {};
    }
    const auto & stripe_info = *stripe_info_ptr;
    size_t num_streams = stripe_info.getNumberOfStreams();

    std::map<uint64_t, size_t> col_id_to_size;
    for (size_t i = 0; i < num_streams; i++)
    {
        const auto stream_info = stripe_info.getStreamInformation(i);
        auto col_id = stream_info->getColumnId();
        col_id_to_size[col_id] += stream_info->getLength();
    }
    std::unordered_map<String, ColumnSize> ret;
    for (size_t i = 0; i < root_type.getSubtypeCount(); i++)
    {
        const auto & name = root_type.getFieldName(i);
        auto col_id = root_type.getSubtype(i)->getColumnId();
        auto it = col_id_to_size.find(col_id);
        if (it != col_id_to_size.end())
        {
            auto & column_size = ret[name];
            column_size.marks = 0;
            column_size.data_compressed = it->second;
            column_size.data_uncompressed = column_size.data_compressed * 15;
        }
        else
        {
            auto & column_size = ret[name];
            column_size.data_compressed = 2 * 1024 * 1024 * 1024L; // 2G
            column_size.data_uncompressed = column_size.data_compressed * 15;
        }
    }
    return ret;
}
}
#endif
