#include <IO/S3Common.h>
#include <gtest/gtest.h>


using namespace DB;

TEST(S3Common, UriRegionParse)
{
    S3::URI s3("s3://bucket-name/key_name");
    ASSERT_EQ(s3.region, "");
    S3::URI s3_http("https://s3.region-code.amazonaws.com/bucket-name/key-name", true);
    ASSERT_EQ(s3_http.region, "region-code");
    S3::URI s3_http_virtual_host("https://bucket-name.s3.region-code.amazonaws.com/key-name", true);
    ASSERT_EQ(s3_http_virtual_host.region, "region-code");

    S3::URI tos_http("http://tos-region-code.ivolces.com/bucket-name/key-name", true);
    ASSERT_EQ(tos_http.region, "region-code");
    S3::URI tos_http_virtual_host("http://bucket-name.tos-region-code.ivolces.com/key-name", true);
    ASSERT_EQ(tos_http_virtual_host.region, "region-code");
    S3::URI tos_http_s3("http://tos-s3-region-code.ivolces.com/bucket-name/key-name", true);
    ASSERT_EQ(tos_http_s3.region, "region-code");
    S3::URI tos_http_s3_virtual_host("http://bucket-name.tos-s3-region-code.ivolces.com/key-name", true);
    ASSERT_EQ(tos_http_s3_virtual_host.region, "region-code");

    S3::URI cos_http("http://cos.region-code.myqcloud.com/bucket-name/key-name", true);
    ASSERT_EQ(cos_http.region, "region-code");
    S3::URI cos_http_virtual_host("http://bucket-name.cos.region-code.myqcloud.com/key-name", true);
    ASSERT_EQ(cos_http_virtual_host.region, "region-code");
}

