#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

/**
 * Map data type in ByteDance.
 * Map can be in different mode:
 * - flatten mode: map is flatten as implicit key columns, __map__key
 * - compacted mode: map is flatten as implicit key columns and all key columns files
 *   are compacted in one file to avoid too many small files in the system
 * - KV mode: map is implemented as two array of keys and values, but it is slightly 
 *   different from community format, i.e. it is Tuple(offset, key, value)
 * */
class DataTypeByteMap final : public IDataType
{
private:
    /// The type of array elements.
    DataTypePtr keyType;
    DataTypePtr valueType;
    DataTypePtr keyStoreType;
    DataTypePtr valueStoreType;

    // "nested" is Tuple( key, value)
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    DataTypeByteMap(const DataTypePtr & keyType_, const DataTypePtr & valueType_);
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Map"; }
    TypeIndex getTypeId() const override { return TypeIndex::ByteMap; }
    bool canBeInsideNullable() const override { return false; }

    /*
    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const IColumn & column) const override;
    SerializationPtr getSubcolumnSerialization(const String & subcolumn_name,
            const BaseSerializationGetter & base_serialization_getter) const override;
    */

#if 0
    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, String field_name, bool allow_add_row, bool & row_added) const;

#endif

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool isComparable() const override { return false; }
    bool haveSubtypes() const override { return true; }

    bool textCanContainOnlyValidUTF8() const override { return keyType->textCanContainOnlyValidUTF8() && valueType->textCanContainOnlyValidUTF8(); }
    bool isMap() const override { return true; }
    bool valueTypeIsLC() const { return valueType->lowCardinality(); }

    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override
    {
        return keyType->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() &&
               valueType->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
    }

    const DataTypePtr & getKeyType() const { return keyType; }
    const DataTypePtr & getValueType() const { return valueType; }
    const DataTypePtr & getKeyStoreType() const { return keyStoreType; }
    const DataTypePtr & getValueStoreType() const { return valueStoreType; }

    NameAndTypePair getValueNameAndType(const String & name) const;
    NameAndTypePair getValueNameAndTypeOfKV(const String & name) const;
    static NameAndTypePair getValueNameAndTypeFromImplicitName(const NameAndTypePair & map_column, const String & name);

    SerializationPtr doGetDefaultSerialization() const override;
};

ColumnPtr mapOffsetsToSizes(const IColumn & column);
}
