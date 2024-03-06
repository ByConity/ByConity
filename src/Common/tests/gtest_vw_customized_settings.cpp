#include <googletest/googletest/include/gtest/gtest.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include "Core/Settings.h"
#include "Core/Types.h"
#include "Interpreters/Context.h"
#include <sstream>
#include <Common/Config/VWCustomizedSettings.h>
#include <Common/tests/gtest_global_context.h>

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
    Settings default_settings;
    DB::ContextMutablePtr local_context = Context::createCopy(getContext().context);
    local_context->setSetting("enable_optimizer", true);
    ASSERT_TRUE(local_context->getSettingsRef().enable_optimizer);
    vw_customized_settings.overwriteDefaultSettings("vw2", local_context);
    ASSERT_FALSE(local_context->getSettingsRef().enable_optimizer);
}
