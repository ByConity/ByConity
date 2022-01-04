#include <DataTypes/DataTypeByteMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/SerializationByteMap.h>
#include <Parsers/IAST.h>
#include <Columns/ColumnByteMap.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Formats/ProtobufReader.h>
#include <Common/FieldVisitors.h>
#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>
#include <common/logger_useful.h>
#include <map>

namespace DB
{

std::string DataTypeByteMap::doGetName() const
{
    return "Map(" + keyType->getName() + ", " + valueType->getName() +  ")";
}

DataTypeByteMap::DataTypeByteMap(const DataTypePtr& keyType_, const DataTypePtr& valueType_)
    : keyType(keyType_), valueType(valueType_)
{
    keyStoreType = std::make_shared<DataTypeArray>(keyType);
    valueStoreType = std::make_shared<DataTypeArray>(valueType);

    nested = std::make_shared<DataTypeTuple>(DataTypes{keyType, valueType}, Names{"keys", "values"});
}

/*
DataTypePtr DataTypeByteMap::tryGetSubcolumnType(const String & subcolumn_name) const
{
    if (subcolumn_name == "size")
        return std::make_shared<DataTypeUInt64>();

    return nested->tryGetSubcolumnType(subcolumn_name);
}

ColumnPtr DataTypeByteMap::getSubcolumn(const String & subcolumn_name, const IColumn& column) const
{
    const auto & column_map = assert_cast<const ColumnByteMap &>(column); 
    if (subcolumn_name == "size")
        return bytemapOffsetsToSizes(column_map.getOffsetsColumn());
    // it depends on how ColumnByteMap is represented in memory
    return nested->getSubcolumn(subcolumn_name, assert_cast<const ColumnByteMap &>(column).getNestedColumn());
}

SerializationPtr DataTypeByteMap::getSubcolumnSerialization(const String & subcolumn_name,
        const BaseSerializationGetter & base_serialization_getter) const
{
    if (subcolumn_name == "size")
        return std::make_shared<SerilizationTupleElement>(base_serialization_getter(DataTypeUInt64()), subcolumn_name);

    return nested->getSubcolumnSerilization(subcolumn_name, base_serialization_getter);
}
*/

SerializationPtr DataTypeByteMap::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationByteMap>(
            keyType->getDefaultSerialization(),
            valueType->getDefaultSerialization()
            /*, nested->getDefaultSerialization()*/);
}

MutableColumnPtr DataTypeByteMap::createColumn() const
{
    return ColumnByteMap::create(keyType->createColumn(), valueType->createColumn());
}

Field DataTypeByteMap::getDefault() const
{
    return ByteMap();
}

bool DataTypeByteMap::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) &&
        keyType->equals(*static_cast<const DataTypeByteMap &>(rhs).keyType) &&
        valueType->equals(*static_cast<const DataTypeByteMap &>(rhs).valueType);
}

NameAndTypePair DataTypeByteMap::getValueNameAndType(const String & name) const
{
    NameAndTypePair res_name_type;

    res_name_type.name = name;
    auto map_value_type = getValueType();

    if (map_value_type->lowCardinality())
        res_name_type.type = map_value_type;
    else
        res_name_type.type = makeNullable(map_value_type);
    
    return res_name_type;
}

NameAndTypePair DataTypeByteMap::getValueNameAndTypeOfKV(const String & name) const
{
    NameAndTypePair res_name_type;
    res_name_type.name = name;

    if (endsWith(name, ".key"))
        res_name_type.type = getKeyStoreType();
    else if (endsWith(name, ".value"))
        res_name_type.type = getValueStoreType();
    else
        throw Exception(name + " is not a valid KV column", ErrorCodes::LOGICAL_ERROR);
    
    return res_name_type;
}

NameAndTypePair DataTypeByteMap::getValueNameAndTypeFromImplicitName(const NameAndTypePair & map_column, const String & name)
{
    if (map_column.type && map_column.type->isMap())
    {
        auto map_type = typeid_cast<const DataTypeByteMap *>(map_column.type.get());
        if (map_type->isMapKVStore())
        {
            return map_type->getValueNameAndTypeOfKV(name);
        }
        else
        {
            return map_type->getValueNameAndType(name);
        }
    }
    else
    {
        throw Exception("Cannot get map value name and type from column " + map_column.name, ErrorCodes::LOGICAL_ERROR);
    }
}

static DataTypePtr create(const ASTPtr& arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Map data type faimily must have two argument-type for key-value pair", ErrorCodes::BAD_ARGUMENTS);

    DataTypePtr keyType = DataTypeFactory::instance().get(arguments->children[0]);
    DataTypePtr valueType = DataTypeFactory::instance().get(arguments->children[1]);
    if (!keyType->canBeMapKVType() || !valueType->canBeMapKVType())
    {
        throw Exception("Map Key or Value Type is not compatible", ErrorCodes::BAD_ARGUMENTS);
    }
    else if (keyType->canBeMapKVType())
    {
        if (typeid_cast<const DataTypeArray *>(keyType.get()))
            throw Exception("Array cannot be map key", ErrorCodes::BAD_ARGUMENTS);
    }

    return std::make_shared<DataTypeByteMap>(keyType, valueType);
}

void registerDataTypeByteMap(DataTypeFactory& factory)
{
    factory.registerDataType("Map", create);
}

ColumnPtr mapOffsetsToSizes(const IColumn & column)
{
    const auto & column_offsets = assert_cast<const ColumnByteMap::ColumnOffsets &>(column);
    MutableColumnPtr column_sizes = column_offsets.cloneEmpty();

    if (column_offsets.empty())
        return column_sizes;

    const auto & offsets_data = column_offsets.getData();
    auto & sizes_data = assert_cast<ColumnByteMap::ColumnOffsets &>(*column_sizes).getData();

    sizes_data.resize(offsets_data.size());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0, size = offsets_data.size(); i < size; ++i)
    {
        auto current_offset = offsets_data[i];
        sizes_data[i] = current_offset - prev_offset;
        prev_offset =  current_offset;
    }

    return column_sizes;
}

}
