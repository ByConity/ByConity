#include <unordered_map>
#include <Poco/Util/ConfigurationView.h>
#include <Common/Config/ConfigProcessor.h>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class VWCustomizedSettings
{
public:
    using ConfigurationViewPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    explicit VWCustomizedSettings(Poco::Util::AbstractConfiguration & config_)
        : vw_settings_prefix("vw_customized_settings"), config_holder(config_.createView(vw_settings_prefix))
    {}

    explicit VWCustomizedSettings(ConfigurationPtr & config_)
        : vw_settings_prefix("vw_customized_settings"), config_holder(config_->createView(vw_settings_prefix))
    {}

    ~VWCustomizedSettings()
    {
        vw_config_keys.clear();
    }

    void overwriteDefaultSettings(const String & vw_name, Settings & default_settings);
    void loadCustomizedSettings();

    String toString();
    bool isEmpty();

private:
    using RawConfigKeys = std::unordered_map<String, std::unordered_map<String, String>>;
    String vw_settings_prefix;
    ConfigurationViewPtr config_holder;
    RawConfigKeys vw_config_keys;
};

using VWCustomizedSettingsPtr = std::shared_ptr<VWCustomizedSettings>;
}
