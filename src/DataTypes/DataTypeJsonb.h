#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

class DataTypeJsonb final : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    TypeIndex getTypeId() const override { return TypeIndex::JSONB; }
    const char * getFamilyName() const override { return "JSONB"; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return false; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}

