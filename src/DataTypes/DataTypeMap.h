#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Map data type.
  * Map is implemented as two arrays of keys and values.
  * Serialization of type 'Map(K, V)' is similar to serialization of 'Array(Tuple(keys K, values V))' or in other
  * words of 'Nested(keys K, valuev V)', to distinguish from ByteMap, we call it KV Map.
  * 
  * Map data type in ByteDance: ByteMap
  * ByteMap can only be flatten mode in cnch, which will write each key to a implicit column like __map__key sparately.
  * 
  * In memory, we use DataTypeMap to represent both Map and ByteMap types.
  * 
  * In column declaration, using Map(K, V) KV to declare a KV Map column, Map(K, V) BYTE to declare a ByteMap column. 
  * Default serialization type is controlled by default_use_kv_map_type.
  * 
  * Because of main difference of ByteMap and KV Map is serialization, so when DataTypeMap is nested in other types, it
  * can only be Map, since enumerateStreams will be called by the outter type when reading/writing it.
  */
class DataTypeMap final : public IDataType
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;

    /// 'nested' is an Array(Tuple(key_type, value_type))
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeMap(const DataTypePtr & nested_);
    DataTypeMap(const DataTypes & elems);
    DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_);

    TypeIndex getTypeId() const override { return TypeIndex::Map; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Map"; }

    bool canBeInsideNullable() const override { return true; }

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const IColumn & column) const override;
    // SerializationPtr getSubcolumnSerialization(
    //     const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
    /// Byte Map can be comparable, but not suitable to be key columns in storage. See KeyDescription::getSortingKeyFromAST
    bool isComparable() const override { return key_type->isComparable() && value_type->isComparable(); }
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    bool textCanContainOnlyValidUTF8() const override;
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override;

    bool isMap() const override { return true; }
    bool isKVMap() const override { return isMapKVStore() || (!isMapByteStore() && getDefaultUseMapType()); }
    bool isByteMap() const override { return !isKVMap(); }

    bool hasNestedMap() const override { return nested->hasNestedMap(); }

    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    DataTypePtr getValueTypeForImplicitColumn() const;
    DataTypes getKeyValueTypes() const { return {key_type, value_type}; }
    const DataTypePtr & getNestedType() const { return nested; }

    SerializationPtr doGetDefaultSerialization() const override;

    void checkValidity() const;
    void checkKeyType() const;

private:
    bool isMapKVStore() const { return flags & TYPE_MAP_KV_STORE_FLAG;}
    bool isMapByteStore() const { return flags & TYPE_MAP_BYTE_STORE_FLAG; }
};

}

