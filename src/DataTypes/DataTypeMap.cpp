#include <common/map.h>
#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

DataTypeMap::DataTypeMap(const DataTypePtr & nested_)
    : nested(nested_)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!type_array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    if (!type_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    key_type = type_tuple->getElement(0);
    value_type = type_tuple->getElement(1);
    checkKeyType();
}

DataTypeMap::DataTypeMap(const DataTypes & elems_)
{
    assert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    checkKeyType();

    nested = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"key", "value"}));
}

DataTypeMap::DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_)
    : key_type(key_type_), value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"key", "value"})))
{
    checkKeyType();
}

void DataTypeMap::checkKeyType() const
{
    if (!key_type->canBeMapKeyType())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Map Key Type {} is not compatible", key_type->getName());
}

void DataTypeMap::checkValidity() const
{
    if (isMapKVStore() && isMapByteStore())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Map KV flag and BYTE flag cannot be set at the same time.");
    if (isByteMap())
    {
        if (!value_type->canBeByteMapValueType())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Map Value Type {} is not compatible", value_type->getName());
    }
}

std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getName() << ", " << value_type->getName() << ")";

    return s.str();
}

static const IColumn & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnMap &>(column).getNestedColumn();
}

DataTypePtr DataTypeMap::tryGetSubcolumnType(const String & subcolumn_name) const
{
    return nested->tryGetSubcolumnType(subcolumn_name);
}

ColumnPtr DataTypeMap::getSubcolumn(const String & subcolumn_name, const IColumn & column) const
{
    return nested->getSubcolumn(subcolumn_name, extractNestedColumn(column));
}

// SerializationPtr DataTypeMap::getSubcolumnSerialization(
//     const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const
// {
//     return nested->getSubcolumnSerialization(subcolumn_name, base_serialization_getter);
// }

MutableColumnPtr DataTypeMap::createColumn() const
{
    return ColumnMap::create(nested->createColumn());
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

/// If type is not nullable, wrap it to be nullable
static DataTypePtr tryMakeNullableForMapValue(const DataTypePtr & type)
{
    if (type->isNullable())
        return type;
    /// When type is low cardinality, its dictionary type must be nullable, see more detail in DataTypeLowCardinality::canBeByteMapValueType()
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(low_cardinality_type->getDictionaryType().get()))
            return type;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not get map implicit column data type of lowcardinality type.");
    }
    else
    {
        if (type->canBeInsideNullable())
            return makeNullable(type);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not get map implicit column data type of type {}.", type->getName());
    }
}

DataTypePtr DataTypeMap::getValueTypeForImplicitColumn() const 
{
    return tryMakeNullableForMapValue(value_type);
}

SerializationPtr DataTypeMap::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationMap>(
        key_type->getDefaultSerialization(), value_type->getDefaultSerialization(), nested->getDefaultSerialization());
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);
    return nested->equals(*rhs_map.nested);
}

bool DataTypeMap::textCanContainOnlyValidUTF8() const
{
    return key_type->textCanContainOnlyValidUTF8() && value_type->textCanContainOnlyValidUTF8();
}

bool DataTypeMap::isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const
{
    return key_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion()
        && value_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Map data type family must have two arguments: key and value types", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeMap>(nested_types);
}

void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create, DataTypeFactory::CaseInsensitive);
}
}
