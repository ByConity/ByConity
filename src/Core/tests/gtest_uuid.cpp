#include <gtest/gtest.h>
#include <Core/UUID.h>
#include <Common/Exception.h>

namespace
{
using namespace DB;

TEST(UUIDHelperTest, UUIDToString)
{
    UUID uuid_input{UInt128{0, 1}};
    std::string uuid_str = UUIDHelpers::UUIDToString(uuid_input);
    EXPECT_EQ(uuid_str, "00000000-0000-0000-0000-000000000001");
    UUID uuid_output = UUIDHelpers::toUUID(uuid_str);
    EXPECT_EQ(uuid_output, uuid_input);
    EXPECT_THROW(UUIDHelpers::toUUID("3"), Exception);
}

} /// end anonymous namespace
