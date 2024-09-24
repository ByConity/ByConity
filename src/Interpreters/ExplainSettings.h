#include <Parsers/ASTSetQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
    extern const int LOGICAL_ERROR;
}

/// Settings. Different for each explain type.

struct QueryMetadataSettings
{
    bool json = false;
    bool lineage = false;
    bool format_json = false;
    bool lineage_use_optimizer = false;
    bool ignore_format = false;

    constexpr static char name[] = "METADATA";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"json", json},
        {"lineage", lineage},
        {"lineage_use_optimizer", lineage_use_optimizer},
        {"format_json", format_json},
        {"ignore_format", ignore_format}
    };
};

struct QueryPlanSettings
{
    QueryPlan::ExplainPlanOptions query_plan_options;

    /// Apply query plan optimizations.
    bool optimize = true;
    bool json = false;
    bool stats = true;
    bool cost = true;
    bool profile = true;
    bool pb_json = false;
    bool verbose = true;
    bool add_whitespace = true; // used to pretty print json
    bool aggregate_profiles = true; 

    constexpr static char name[] = "PLAN";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings = {
        {"header", query_plan_options.header},
        {"description", query_plan_options.description},
        {"actions", query_plan_options.actions},
        {"indexes", query_plan_options.indexes},
        {"optimize", optimize},
        {"json", json},
        {"pb_json", pb_json},
        {"stats", stats},
        {"cost", cost},
        {"profile", profile},
        {"add_whitespace", add_whitespace},
        {"aggregate_profiles", aggregate_profiles},
        {"verbose", verbose},
    };
};

struct QueryPipelineSettings
{
    QueryPlan::ExplainPipelineOptions query_pipeline_options;
    bool graph = false;
    bool compact = true;
    bool stats = true;

    constexpr static char name[] = "PIPELINE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_pipeline_options.header},
            {"graph", graph},
            {"compact", compact},
            {"stats", stats}
    };
};

template <typename Settings>
struct ExplainSettings : public Settings
{
    using Settings::boolean_settings;

    bool has(const std::string & name_) const
    {
        return boolean_settings.count(name_) > 0;
    }

    void setBooleanSetting(const std::string & name_, bool value)
    {
        auto it = boolean_settings.find(name_);
        if (it == boolean_settings.end())
            throw Exception("Unknown setting for ExplainSettings: " + name_, ErrorCodes::LOGICAL_ERROR);

        it->second.get() = value;
    }

    std::string getSettingsList() const
    {
        std::string res;
        for (const auto & setting : boolean_settings)
        {
            if (!res.empty())
                res += ", ";

            res += setting.first;
        }

        return res;
    }
};

template <typename Settings>
ExplainSettings<Settings> checkAndGetSettings(const ASTPtr & ast_settings)
{
    if (!ast_settings)
        return {};

    ExplainSettings<Settings> settings;
    const auto & set_query = ast_settings->as<ASTSetQuery &>();

    for (const auto & change : set_query.changes)
    {
        if (!settings.has(change.name))
            throw Exception("Unknown setting \"" + change.name + "\" for EXPLAIN " + Settings::name + " query. "
                            "Supported settings: " + settings.getSettingsList(), ErrorCodes::UNKNOWN_SETTING);

        if (change.value.getType() != Field::Types::UInt64)
            throw Exception("Invalid type " + std::string(change.value.getTypeName()) + " for setting \"" + change.name +
                            "\" only boolean settings are supported", ErrorCodes::INVALID_SETTING_VALUE);

        auto value = change.value.get<UInt64>();
        if (value > 1)
            throw Exception("Invalid value " + std::to_string(value) + " for setting \"" + change.name +
                            "\". Only boolean settings are supported", ErrorCodes::INVALID_SETTING_VALUE);

        settings.setBooleanSetting(change.name, value);
    }

    return settings;
}

}
