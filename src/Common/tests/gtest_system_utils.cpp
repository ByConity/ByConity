#include <iostream>
#include <string>

#include <gtest/gtest.h>

#include <Common/SystemUtils.h>

TEST(CGroup, ParseNumaInfo)
{
    std::string s = "12345";
    ASSERT_EQ(DB::buffer_to_number(s), 12345);

    s = "0-2";
    ASSERT_EQ(DB::buffer_to_number(s), 2);

    s = "0-2,3-6";
    ASSERT_EQ(DB::buffer_to_number(s), 6);

    s = "";
    ASSERT_EQ(DB::buffer_to_number(s), 0);

    s = "asdf";
    ASSERT_EQ(DB::buffer_to_number(s), 0);

    s = "1234523123123123123123123123012031023123123123539834712908";
    ASSERT_EQ(DB::buffer_to_number(s), 0);
}
