#pragma once

#include <Core/Field.h>


namespace DB
{

/** StaticVisitor (and its descendants) - class with overloaded operator() for all types of fields.
  * You could call visitor for field using function 'applyVisitor'.
  * Also "binary visitor" is supported - its operator() takes two arguments.
  */
template <typename R = void>
struct StaticVisitor
{
    using ResultType = R;
};

/// F is template parameter, to allow universal reference for field, that is useful for const and non-const values.
template <typename Visitor, typename F>
typename std::decay_t<Visitor>::ResultType applyVisitorExplicit(Visitor && visitor, F && field)
{
    switch (field.getType())
    {
        case Field::Types::Null: return visitor(field.template get<Null>());
        case Field::Types::UInt64: return visitor(field.template get<UInt64>());
        case Field::Types::UInt128: return visitor(field.template get<UInt128>());
        case Field::Types::UInt256: return visitor(field.template get<UInt256>());
        case Field::Types::Int64: return visitor(field.template get<Int64>());
        case Field::Types::Float64: return visitor(field.template get<Float64>());
        case Field::Types::String: return visitor(field.template get<String>());
        case Field::Types::Array: return visitor(field.template get<Array>());
        case Field::Types::Tuple: return visitor(field.template get<Tuple>());
        case Field::Types::Decimal32: return visitor(field.template get<DecimalField<Decimal32>>());
        case Field::Types::Decimal64: return visitor(field.template get<DecimalField<Decimal64>>());
        case Field::Types::Decimal128: return visitor(field.template get<DecimalField<Decimal128>>());
        case Field::Types::Decimal256: return visitor(field.template get<DecimalField<Decimal256>>());
        case Field::Types::AggregateFunctionState: return visitor(field.template get<AggregateFunctionStateData>());
#ifdef HAVE_BOO_TYPE
        case Field::Types::Bool:
            return visitor(field.template get<bool>());
#endif
        case Field::Types::Object: return visitor(field.template get<Object>());
        case Field::Types::Map:     return visitor(field.template get<Map>());
        case Field::Types::BitMap64:
            return visitor(field.template get<BitMap64>());

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}

/// F is template parameter, to allow universal reference for field, that is useful for const and non-const values.
template <typename Visitor, typename F>
auto applyVisitor(Visitor && visitor, F && field)
{
    return Field::dispatch(std::forward<Visitor>(visitor),
        std::forward<F>(field));
}

template <typename Visitor, typename F1, typename F2>
auto applyVisitor(Visitor && visitor, F1 && field1, F2 && field2)
{
    return Field::dispatch(
        [&field2, &visitor](auto & field1_value)
        {
            return Field::dispatch(
                [&field1_value, &visitor](auto & field2_value)
                {
                    return visitor(field1_value, field2_value);
                },
                std::forward<F2>(field2));
        },
        std::forward<F1>(field1));
}

}
