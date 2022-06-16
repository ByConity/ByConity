#include <Analyzers/TypeCoercion.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

using TypeCompatibility = TypeCoercion::TypeCompatibility;

static std::optional<DataTypePtr> makeNullable(const std::optional<DataTypePtr> & data_type)
{
    if (data_type)
        return ::DB::makeNullable(data_type.value());
    else
        return {};
}

TypeCompatibility TypeCoercion::compatibility(const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    if (from_type->equals(*to_type))
    {
        return TypeCompatibility{to_type, true};
    }

    const TypeIndex & from_type_id = from_type->getTypeId();
    const TypeIndex & to_type_id = to_type->getTypeId();

    /// DataTypeNullable
    if (from_type_id == TypeIndex::Nullable)
    {
        auto type_compatibility = compatibility(typeid_cast<const DataTypeNullable &>(*from_type).getNestedType(), to_type);
        // nullable can not cast into non-nullable
        return TypeCompatibility{
            makeNullable(type_compatibility.getCommonSuperType()), to_type_id == TypeIndex::Nullable && type_compatibility.isCoercible()};
    }
    if (to_type_id == TypeIndex::Nullable)
    {
        auto type_compatibility = compatibility(from_type, typeid_cast<const DataTypeNullable &>(*to_type).getNestedType());
        return TypeCompatibility{makeNullable(type_compatibility.getCommonSuperType()), type_compatibility.isCoercible()};
    }

    if (from_type_id != to_type_id)
        return TypeCompatibility::incompatible();

    if (from_type_id == TypeIndex::Tuple)
        return typeCompatibilityForTuple(typeid_cast<const DataTypeTuple &>(*from_type), typeid_cast<const DataTypeTuple &>(*to_type));

    // todo: implement more types
    // like StandardTypes.DECIMAL, StandardTypes.VARCHAR, StandardTypes.CHAR
    // CovariantParametrizedType
    return TypeCompatibility::incompatible();
}

TypeCompatibility TypeCoercion::typeCompatibilityForTuple(const DB::DataTypeTuple & first_type, const DB::DataTypeTuple & second_type)
{
    const auto & first_elems = first_type.getElements();
    const auto & second_elems = second_type.getElements();
    const auto & first_names = first_type.getElementNames();
    const auto & second_names = first_type.getElementNames();

    if (first_elems.size() != second_elems.size())
    {
        return TypeCompatibility::incompatible();
    }

    DataTypes types;
    Strings names;
    bool coercible = true;
    for (size_t i = 0; i < first_elems.size(); i++)
    {
        auto type_compatibility = compatibility(first_elems[i], second_elems[i]);
        const auto & common_parameter_type = type_compatibility.getCommonSuperType();
        if (!common_parameter_type)
        {
            return TypeCompatibility::incompatible();
        }

        types.emplace_back(common_parameter_type.value());
        coercible &= type_compatibility.isCoercible();

        if (first_type.haveExplicitNames())
            names.emplace_back(first_names[i]);
        else if (second_type.haveExplicitNames())
            names.emplace_back(second_names[i]);
    }

    if (first_type.haveExplicitNames() || second_type.haveExplicitNames())
        return TypeCompatibility{std::make_shared<DataTypeTuple>(types, names), coercible};
    else
        return TypeCompatibility{std::make_shared<DataTypeTuple>(types), coercible};
}


TypeCoercion::TypeCompatibility::TypeCompatibility(std::optional<DataTypePtr> common_super_type_, bool coercible_)
    : common_super_type(std::move(common_super_type_)), coercible(coercible_)
{
    // Assert that: coercible => commonSuperType.isPresent
    assert(!coercible || common_super_type);
}

}
