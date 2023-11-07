#include <Protos/EnumMacros.h>
#include <Protos/RPCHelpers.h>
#include <Protos/plan_node.pb.h>
#include <gtest/gtest.h>
#include "Interpreters/DistributedStages/ExchangeMode.h"

namespace
{
using namespace DB;


class TestingClass
{
public:
    ENUM_WITH_PROTO_CONVERTER(
        ExchangeMode,
        Protos::ExchangeMode,
        (UNKNOWN, 10), // use just like UNKNOWN = 10,
        (LOCAL_NO_NEED_REPARTITION), // any comment here
        (LOCAL_MAY_NEED_REPARTITION),
        (REPARTITION),
        (BROADCAST),
        (GATHER));
};


TEST(EnumMacros, TupleTest)
{
    using ExchangeMode = TestingClass::ExchangeMode;
    // <ENUM_NAME> + Converter will be the helping class, in the same scope with enum
    // currently support toProto, fromProto, toString
    // see the preprocessed code in the last section
    using ExchangeModeConverter = TestingClass::ExchangeModeConverter;

    auto mode = ExchangeMode::GATHER;
    ASSERT_EQ((int)mode, 15);
    auto mode_proto = ExchangeModeConverter::toProto(mode);
    ASSERT_EQ((int)mode_proto, 5);
    auto mode2 = ExchangeModeConverter::fromProto(mode_proto);
    ASSERT_EQ(mode, mode2);
}

}

// after preprocessing, ENUM_WITH_PROTO_CONVERTER will be expanded into the following code
// you can check this with the following command:
// cd ${CLICKHOUSE_HOME}
// clang++-11 -E --preprocess -I ./contrib/boost -I ./src ./src/Protos/test/gtest_enum_macros.cpp | clang-format-15

#if 0
class TestingClass
{
public:
    enum class ExchangeMode
    {
        UNKNOWN = 10,
        LOCAL_NO_NEED_REPARTITION,
        LOCAL_MAY_NEED_REPARTITION,
        REPARTITION,
        BROADCAST,
        GATHER,
    };
    class ExchangeModeConverter
    {
    public:
        using _enumType = ExchangeMode;
        static Protos::ExchangeMode::Enum toProto(const _enumType & _value)
        {
            switch (_value)
            {
                case _enumType::UNKNOWN:
                    return Protos::ExchangeMode::UNKNOWN;
                case _enumType::LOCAL_NO_NEED_REPARTITION:
                    return Protos::ExchangeMode::LOCAL_NO_NEED_REPARTITION;
                case _enumType::LOCAL_MAY_NEED_REPARTITION:
                    return Protos::ExchangeMode::LOCAL_MAY_NEED_REPARTITION;
                case _enumType::REPARTITION:
                    return Protos::ExchangeMode::REPARTITION;
                case _enumType::BROADCAST:
                    return Protos::ExchangeMode::BROADCAST;
                case _enumType::GATHER:
                    return Protos::ExchangeMode::GATHER;
                default: {
                    throwBetterEnumException("cpp enum", "ExchangeMode", static_cast<int>(_value));
                }
            }
        }
        static ExchangeMode fromProto(const Protos::ExchangeMode::Enum & proto)
        {
            switch (proto)
            {
                case Protos::ExchangeMode::UNKNOWN:
                    return _enumType::UNKNOWN;
                case Protos::ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                    return _enumType::LOCAL_NO_NEED_REPARTITION;
                case Protos::ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                    return _enumType::LOCAL_MAY_NEED_REPARTITION;
                case Protos::ExchangeMode::REPARTITION:
                    return _enumType::REPARTITION;
                case Protos::ExchangeMode::BROADCAST:
                    return _enumType::BROADCAST;
                case Protos::ExchangeMode::GATHER:
                    return _enumType::GATHER;
                default: {
                    throwBetterEnumException("protobuf", "ExchangeMode", static_cast<int>(proto));
                }
            }
        }
        static const std::string & toString(const _enumType & _value) { return Protos::ExchangeMode::Enum_Name(toProto(_value)); }
    };
#     25 "../src/Protos/test/gtest_enum_macros.cpp"
};
#endif
