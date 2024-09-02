#include <Core/SettingsFields.h>
#include <Statistics/SettingsMap.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Common/SettingsChanges.h>


namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int INVALID_SETTING_VALUE;
}
namespace DB::Statistics
{
String SettingsMap::toJsonStr() const
{
    if (empty())
        return "";

    Poco::JSON::Object json;
    for (const auto & [k, v] : *this)
    {
        json.set(k, v);
    }

    std::stringstream ss;
    json.stringify(ss);
    return ss.str();
}

void SettingsMap::fromJsonStr(const String & json_str)
{
    if (json_str.empty())
        return;

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json_str);
    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
    for (auto & [key, value] : *object)
    {
        auto v = value.convert<String>();
        this->operator[](key) = v;
    }
}

void AutoStatsManagerSettings::applySettingsChanges(const SettingsChanges & changes, bool throw_if_override)
{
    std::unordered_map<String, Field> changes_map;
    for (auto change : changes)
    {
        if (changes_map.count(change.name))
        {
            auto err_msg = fmt::format("duplicate key {}", change.name);
            throw Exception(err_msg, ErrorCodes::SYNTAX_ERROR);
        }
        changes_map.emplace(change.name, change.value);
    }

    auto insert_key = [&]<typename SettingFieldType>(const String & key) {
        if (changes_map.count(key))
        {
            SettingFieldType tmp;
            if (throw_if_override && changes_map.contains(key))
            {
                auto err_msg = fmt::format("`{}` overrides previous settings, not allowed", key);
                throw Exception(err_msg, ErrorCodes::INVALID_SETTING_VALUE);
            }
            auto field = changes_map[key];
            tmp = field;
            auto str = tmp.toString();
            this->operator[](key) = str;
            changes_map.erase(key);
        }
    };

#define INSERT_KEY(TYPE, NAME, DEFAULT_VALUE, EXPLAIN, INTERNAL) \
    if constexpr (!(INTERNAL)) \
    { \
        insert_key.operator()<SettingField##TYPE>(#NAME); \
    }
    AUTO_STATS_MANAGER_SETTINGS(INSERT_KEY)
#undef INSERT_KEY

    if (!changes_map.empty())
    {
        auto err_msg = fmt::format("unknown setting key {}", changes_map.begin()->first);
        throw Exception(err_msg, ErrorCodes::INVALID_SETTING_VALUE);
    }
}

void CreateStatsSettings::normalize()
{
    if (histogram_bucket_size() <= 0)
    {
        set_collect_histogram(false);
    }
}

void CreateStatsSettings::fromContextSettings(const Settings & settings)
{
#define SET_KEY(TYPE, NAME, DEFAULT_VALUE, EXPLAIN, INTERNAL) this->set_##NAME(settings.statistics_##NAME);

    CREATE_STATS_SETTINGS(SET_KEY)
#undef SET_KEY

    normalize();
}

// only for statistics
void applyStatisticsSettingsChanges(Settings & settings, SettingsChanges settings_changes)
{
    for (auto & change : settings_changes)
    {
        // when use with sample_ratio=xxx, support it
        if (!change.name.starts_with("statistics_"))
        {
            change.name = "statistics_" + change.name;
        }
    }
    settings.applyChanges(settings_changes);
}
}
