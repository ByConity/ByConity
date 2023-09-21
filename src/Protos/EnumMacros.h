#pragma once
#include <string>
#include <boost/preprocessor.hpp>

/// see documents and sample code in Protos/test/gtest_enum_macros.cpp
namespace DB::ErrorCodes
{
extern const int PROTOBUF_BAD_CAST;
}

namespace DB
{
[[noreturn]] void throwBetterEnumException(const char * type, const char * enum_name, int code);
}

#define PP_ENUM_WITH_PROTO_ENUM_DEF_1(_name) _name,
#define PP_ENUM_WITH_PROTO_ENUM_DEF_2(_name, _value) _name = _value,
#define PP_ENUM_WITH_PROTO_ENUM_DEF(...) BOOST_PP_OVERLOAD(PP_ENUM_WITH_PROTO_ENUM_DEF_, __VA_ARGS__)(__VA_ARGS__)

#define PP_ENUM_WITH_PROTO_ENUM_DEF_HELPER(_r, _extra, _tp) PP_ENUM_WITH_PROTO_ENUM_DEF _tp
#define BOOST_PPEX_FIRST_ELEMENT(_r, _n, _tp) BOOST_PP_SEQ_HEAD(BOOST_PP_TUPLE_TO_SEQ(_tp))
#define PP_ENUM_WITH_PROTO_IMPL_CASE(_r, _prefixs, _elem) \
    case BOOST_PP_TUPLE_ELEM(2, 0, _prefixs)::_elem: \
        return BOOST_PP_TUPLE_ELEM(2, 1, _prefixs)::_elem;

#define PP_ENUM_WITH_PROTO_CONVERTER_DEF(ENUM_NAME, PROTO_SCOPE, SEQS) \
    class BOOST_PP_CAT(ENUM_NAME, Converter) \
    { \
    public: \
        using _enumType = ENUM_NAME; \
        [[maybe_unused]] static PROTO_SCOPE::Enum toProto(const _enumType & _value) \
        { \
            switch (_value) \
            { \
                BOOST_PP_SEQ_FOR_EACH(PP_ENUM_WITH_PROTO_IMPL_CASE, (_enumType, PROTO_SCOPE), SEQS) \
                default: { \
                    throwBetterEnumException("cpp enum", BOOST_PP_STRINGIZE(ENUM_NAME), static_cast<int>(_value)); \
                } \
            } \
        } \
        [[maybe_unused]] static _enumType fromProto(const PROTO_SCOPE::Enum & proto) \
        { \
            switch (proto) \
            { \
                BOOST_PP_SEQ_FOR_EACH(PP_ENUM_WITH_PROTO_IMPL_CASE, (PROTO_SCOPE, _enumType), SEQS) \
                default: { \
                    throwBetterEnumException("protobuf", BOOST_PP_STRINGIZE(ENUM_NAME), static_cast<int>(proto)); \
                } \
            } \
        } \
        [[maybe_unused]] static const std::string & toString(const _enumType & _value) { return PROTO_SCOPE::Enum_Name(toProto(_value)); } \
    }

#define ENUM_WITH_PROTO_CONVERTER_IMPL(ENUM_NAME, PROTO_SCOPE, SEQS, ENUM_CLASS_KEYWORD) \
    ENUM_CLASS_KEYWORD ENUM_NAME{BOOST_PP_SEQ_FOR_EACH(PP_ENUM_WITH_PROTO_ENUM_DEF_HELPER, ~, SEQS)}; \
    PP_ENUM_WITH_PROTO_CONVERTER_DEF(ENUM_NAME, PROTO_SCOPE, BOOST_PP_SEQ_TRANSFORM(BOOST_PPEX_FIRST_ELEMENT, ~, SEQS))

/// see documents and sample code in Protos/test/gtest_enum_macros.cpp
#define ENUM_WITH_PROTO_CONVERTER(ENUM_NAME, PROTO_SCOPE, ...) \
    ENUM_WITH_PROTO_CONVERTER_IMPL(ENUM_NAME, PROTO_SCOPE, BOOST_PP_VARIADIC_TO_SEQ(__VA_ARGS__), enum class)

/// this will use 'enum' instead of 'enum class' to define macro
/// the other behaviours is identical to ENUM_WITH_PROTO_CONVERTER
#define ENUM_WITH_PROTO_CONVERTER_C_STYLE(ENUM_NAME, PROTO_SCOPE, ...) \
    ENUM_WITH_PROTO_CONVERTER_IMPL(ENUM_NAME, PROTO_SCOPE, BOOST_PP_VARIADIC_TO_SEQ(__VA_ARGS__), enum)
