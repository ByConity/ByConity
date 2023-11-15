#include <Statistics/CollectorSettings.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

String Statistics::CollectorSettings::toJsonStr() const
{
    Poco::JSON::Object json;
    CollectorSettings default_settings;

    if (collect_histogram != default_settings.collect_histogram)
        json.set("collect_histogram", collect_histogram);

    if (collect_floating_histogram != default_settings.collect_floating_histogram)
        json.set("collect_floating_histogram", collect_floating_histogram);

    if (collect_floating_histogram_ndv != default_settings.collect_floating_histogram_ndv)
        json.set("collect_floating_histogram_ndv", collect_floating_histogram_ndv);

    // we always put enable_sample into json to explicitly remind users
    json.set("enable_sample", enable_sample);

    if (sample_row_count != default_settings.sample_row_count)
        json.set("sample_row_count", sample_row_count);

    if (sample_ratio != default_settings.sample_ratio)
        json.set("sample_ratio", sample_ratio);

    if (accurate_sample_ndv != default_settings.accurate_sample_ndv)
        json.set("accurate_sample_ndv", SettingFieldStatisticsAccurateSampleNdvModeTraits::toString(accurate_sample_ndv));

    if (if_not_exists != default_settings.if_not_exists)
        json.set("if_not_exists", if_not_exists);

    // not useful for creating stats, but serialize it anyway if changed
    if (cache_policy != StatisticsCachePolicy::Default)
        json.set("cache_policy", SettingFieldStatisticsCachePolicyTraits::toString(cache_policy));

    std::stringstream ss;
    json.stringify(ss);
    return ss.str();
}


void Statistics::CollectorSettings::fromJsonStr(const String & json_str)
{
    if (json_str.empty())
        return;

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json_str);
    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

    if (object->has("collect_histogram"))
        collect_histogram = object->get("collect_histogram").convert<bool>();

    if (object->has("collect_floating_histogram"))
        collect_floating_histogram = object->get("collect_floating_histogram").convert<bool>();

    if (object->has("collect_floating_histogram_ndv"))
        collect_floating_histogram_ndv = object->get("collect_floating_histogram_ndv").convert<bool>();

    if (object->has("enable_sample"))
        enable_sample = object->get("enable_sample").convert<bool>();

    if (object->has("sample_row_count"))
        sample_row_count = object->get("sample_row_count").convert<UInt64>();

    if (object->has("sample_ratio"))
        sample_ratio = object->get("sample_ratio").convert<double>();

    if (object->has("accurate_sample_ndv"))
        accurate_sample_ndv
            = SettingFieldStatisticsAccurateSampleNdvModeTraits::fromString(object->get("accurate_sample_ndv").convert<String>());

    if (object->has("if_not_exists"))
        if_not_exists = object->get("if_not_exists").convert<bool>();

    if (object->has("cache_policy"))
        cache_policy = SettingFieldStatisticsCachePolicyTraits::fromString(object->get("cache_policy").convert<String>());
}


}
