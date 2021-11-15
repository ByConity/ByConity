#pragma once

#include <DataTypes/IDataType.h>
#include <Core/Field.h>


namespace DB
{

class DataTypeBitMap64 final : public IDataType
{
public:
    using FieldType = roaring::Roaring64Map;
    static constexpr bool is_parametric = false;

    const char * getFamilyName() const override
    {
        return "BitMap64";
    }

    TypeIndex getTypeId() const override { return TypeIndex::BitMap64; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override
    {
        return Field(BitMap64());
    }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeComparedWithCollation() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return false; }
    bool canBeInsideLowCardinality() const override { return false; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
