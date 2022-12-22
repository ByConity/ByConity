#include <gtest/gtest.h>
#include <Protos/RPCHelpers.h>

namespace
{
using namespace DB;

TEST(RPCHelperTest, UUIDTest)
{
    UUID uuid_input = UUIDHelpers::generateV4();
    Protos::UUID uuid_protos;
    DB::RPCHelpers::fillUUID(uuid_input, uuid_protos);
    UUID uuid_output = DB::RPCHelpers::createUUID(uuid_protos);
    EXPECT_EQ(uuid_output, uuid_input);
}


}
