#include <Optimizer/LiteralEncoder.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/formatAST.h>
#include <Common/FieldVisitorToString.h>
#include <Common/IntervalKind.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <iostream>

using namespace DB;

void testEncode(const Field & field, const DataTypePtr & type, const String & expect, ContextMutablePtr context)
{
    auto res = LiteralEncoder::encode(field, type, context);
    auto res_str = serializeAST(*res);

    if (res_str != expect)
        GTEST_FAIL() << "Literal " << applyVisitor(FieldVisitorToString(), field) << " of type " << type->getName() << " is encoded to "
                     << res_str << ", but expect is " << expect << std::endl;
}

TEST(OptimizerLiteralEncoderTest, testEncode)
{
    auto context = Context::createCopy(getContext().context);
    testEncode(1U, std::make_shared<DataTypeUInt8>(), "1", context);
    testEncode(1U, makeNullable(std::make_shared<DataTypeUInt8>()), "cast(1, 'Nullable(UInt8)')", context);
    testEncode(1U, std::make_shared<DataTypeUInt32>(), "cast(1, 'UInt32')", context);
    testEncode(12345U, std::make_shared<DataTypeDate>(), "cast(12345, 'Date')", context);
    testEncode(666, std::make_shared<DataTypeInterval>(IntervalKind{IntervalKind::Kind::Day}), "toIntervalDay(666)", context);
}
