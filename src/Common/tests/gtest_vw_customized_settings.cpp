#include <googletest/googletest/include/gtest/gtest.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include "Core/Settings.h"
#include "Core/Types.h"
#include "Interpreters/Context.h"
#include <sstream>
#include <Common/Config/VWCustomizedSettings.h>

using namespace DB;

TEST(VWCustomizedSettings, loadCustomizedSettings)
{
    String input_conf = R"(
<yandex>
<vw_customized_settings>
    <vw>
        <name>vw1</name>
        <enable_optimizer>1</enable_optimizer>
    </vw>
    
    <vw>
        <name>vw2</name>
        <enable_optimizer>0</enable_optimizer>
    </vw>
</vw_customized_settings>
</yandex>)";
    std::stringstream input_conf_stream(input_conf);
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> raw_conf_ptr = Poco::AutoPtr(new Poco::Util::XMLConfiguration(input_conf_stream));
    VWCustomizedSettings vw_customized_settings(raw_conf_ptr);
    vw_customized_settings.loadCustomizedSettings();
    Settings default_settings;
    const String conf_key("enable_optimizer");
    const String default_conf_value("1");
    default_settings.set(conf_key, default_conf_value);
    ASSERT_EQ(default_settings.enable_optimizer, true);
    vw_customized_settings.overwriteDefaultSettings("vw2", default_settings);
    ASSERT_EQ(default_settings.enable_optimizer, false);
}
