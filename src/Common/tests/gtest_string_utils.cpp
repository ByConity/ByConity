#include <Common/StringUtils/StringUtils.h>

#include <gtest/gtest.h>

TEST(StringUtils, Hash)
{
    std::string s = "cluster by id INTO 10 BUCKETS";
#if __SIZEOF_SIZE_T__ == 8
    ASSERT_EQ(compatibility::v1::hash(s), 14018056688674197819UL);
#endif
    ASSERT_EQ(compatibility::v2::hash(s), 18243203568518823131UL);
}
