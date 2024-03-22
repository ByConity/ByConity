#include <ostream>
#include <string>
#include <gtest/gtest.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/AdditionalServices.h>
#include <Common/Exception.h>


Poco::AutoPtr<Poco::Util::AbstractConfiguration> stringToConfig(const std::string & str)
{
    std::stringstream ss(str);
    Poco::XML::InputSource input_source{ss};
    return new Poco::Util::XMLConfiguration{&input_source};
}

TEST(AdditionalServices, ParseConfig)
{
    DB::AdditionalServices svcs;
    svcs.parseAdditionalServicesFromConfig(*stringToConfig("<yandex></yandex>"));
    EXPECT_EQ(svcs.size(), 0);

    svcs.parseAdditionalServicesFromConfig(*stringToConfig("<yandex><additional_services></additional_services></yandex>"));
    EXPECT_EQ(svcs.size(), 0);

    svcs.parseAdditionalServicesFromConfig(*stringToConfig("<yandex><additional_services><GIS>1</GIS></additional_services></yandex>"));
    EXPECT_EQ(svcs.size(), 1);

    svcs.parseAdditionalServicesFromConfig(*stringToConfig("<yandex><additional_services><asdf>1</asdf></additional_services></yandex>"));
    EXPECT_EQ(svcs.size(), 0);

    svcs.parseAdditionalServicesFromConfig(*stringToConfig("<yandex><additional_services><GIS>0</GIS></additional_services></yandex>"));
    EXPECT_EQ(svcs.size(), 0);

    DB::AdditionalServices addi_svc;
    addi_svc.parseAdditionalServicesFromConfig(*stringToConfig(
        "<yandex><additional_services><VectorSearch>1</VectorSearch><FullTextSearch>1</FullTextSearch></additional_services></yandex>"));

    EXPECT_EQ(addi_svc.enabled(DB::AdditionalService::Value::GIS), false);
    EXPECT_EQ(addi_svc.enabled(DB::AdditionalService::VectorSearch), true);
    EXPECT_EQ(addi_svc.enabled(DB::AdditionalService::FullTextSearch), true);
    EXPECT_THROW({ addi_svc.throwIfDisabled(DB::AdditionalService::GIS); }, DB::Exception);
}
