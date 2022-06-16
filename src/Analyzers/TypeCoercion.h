#pragma once

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class TypeCoercion
{
public:
    class TypeCompatibility
    {
    public:
        TypeCompatibility(std::optional<DataTypePtr> common_super_type, bool coercible);

        static TypeCompatibility incompatible() { return TypeCompatibility{{}, false}; }

        // whether from_type can be cast to to_type
        bool isCoercible() const { return coercible; }
        bool isCompatible() const { return common_super_type.has_value(); }
        const std::optional<DataTypePtr> & getCommonSuperType() const { return common_super_type; }

    private:
        const std::optional<DataTypePtr> common_super_type;
        const bool coercible;
    };

    static bool canCoerce(const DataTypePtr & from_type, const DataTypePtr & to_type)
    {
        TypeCompatibility type_compatibility = compatibility(from_type, to_type);
        return type_compatibility.isCoercible();
    }

    static TypeCompatibility compatibility(const DataTypePtr & from_type, const DataTypePtr & to_type);

    static TypeCompatibility typeCompatibilityForTuple(const DataTypeTuple & first_type, const DataTypeTuple & second_type);
};

}
