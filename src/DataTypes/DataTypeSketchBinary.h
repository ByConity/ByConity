#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{
class DataTypeSketchBinary final : public IDataType
{
public:
    using FieldType = String;
    static constexpr bool is_parametric = false;
    static constexpr auto type_id = TypeIndex::SketchBinary;

    const char * getFamilyName() const override { return "SketchBinary"; }

    TypeIndex getTypeId() const override { return type_id; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeComparedWithCollation() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }
    bool canBeMapKeyType() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override;
    Field stringToVisitorField(const String& ins) const override;
    String stringToVisitorString(const String & ins) const override;

};

}
