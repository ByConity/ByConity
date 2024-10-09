#include <Storages/StorageS3Settings.h>

#include <IO/S3Common.h>

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/predicate.hpp>
#include <Poco/Logger.h>

namespace DB
{

void StorageS3Settings::loadFromConfig(
    const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings)
{
    std::lock_guard lock(mutex);
    s3_settings.clear();
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    auto get_string_for_key = [&](const String & key, const String & elem, bool with_default = true, const String & default_value = "") {
        return with_default ? config.getString(config_elem + "." + key + "." + elem, default_value)
                            : config.getString(config_elem + "." + key + "." + elem);
    };

    auto get_uint_for_key = [&](const String & key, const String & elem, bool with_default = true, UInt64 default_value = 0) {
        return with_default ? config.getUInt64(config_elem + "." + key + "." + elem, default_value)
                            : config.getUInt64(config_elem + "." + key + "." + elem);
    };


    auto get_bool_for_key = [&](const String & key, const String & elem, bool with_default = true, bool default_value = false) {
        return with_default ? config.getBool(config_elem + "." + key + "." + elem, default_value)
                            : config.getBool(config_elem + "." + key + "." + elem);
    };


    for (const String & key : config_keys)
    {
        if (config.has(config_elem + "." + key + ".endpoint"))
        {
            auto endpoint = get_string_for_key(key, "endpoint", false);

            auto auth_settings = S3::AuthSettings::loadFromConfig(config_elem + "." + key, config);

            S3Settings::RequestSettings request_settings;
            request_settings.max_single_read_retries
                = get_uint_for_key(key, "max_single_read_retries", true, settings.s3_max_single_read_retries);
            request_settings.min_upload_part_size = get_uint_for_key(key, "min_upload_part_size", true, settings.s3_min_upload_part_size);
            request_settings.upload_part_size_multiply_factor
                = get_uint_for_key(key, "upload_part_size_multiply_factor", true, settings.s3_upload_part_size_multiply_factor);
            request_settings.upload_part_size_multiply_parts_count_threshold = get_uint_for_key(
                key, "upload_part_size_multiply_parts_count_threshold", true, settings.s3_upload_part_size_multiply_parts_count_threshold);
            request_settings.max_single_part_upload_size
                = get_uint_for_key(key, "max_single_part_upload_size", true, settings.s3_max_single_part_upload_size);
            request_settings.max_connections = get_uint_for_key(key, "max_connections", true, settings.s3_max_connections);
            request_settings.check_objects_after_upload = get_bool_for_key(key, "check_objects_after_upload", true, false);
            request_settings.max_list_nums = get_uint_for_key(key, "max_list_nums", true, settings.s3_max_list_nums);
            request_settings.max_timeout_ms = get_uint_for_key(key, "max_timeout_ms", true, settings.s3_max_request_ms);
            request_settings.max_unexpected_write_error_retries
                = get_uint_for_key(key, "max_unexpected_write_error_retries", true, settings.s3_max_unexpected_write_error_retries);
            request_settings.s3_use_parallel_upload = get_bool_for_key(key, "s3_use_parallel_upload", true, false);
            request_settings.s3_parallel_upload_pool_size
                = get_uint_for_key(key, "s3_parallel_upload_pool_size", true, settings.s3_parallel_upload_pool_size);

            if (key == "default")
                s3_settings.emplace("default", S3Settings{endpoint, auth_settings, request_settings});

            s3_settings.emplace(endpoint, S3Settings{endpoint, std::move(auth_settings), std::move(request_settings)});
        }
    }
}

S3Settings StorageS3Settings::getSettings(const String & endpoint) const
{
    std::lock_guard lock(mutex);
    if ((endpoint.empty() or endpoint == "default") && s3_settings.find("default") != s3_settings.end())
        return s3_settings.at("default");

    auto next_prefix_setting = s3_settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != s3_settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        if (boost::algorithm::starts_with(endpoint, possible_prefix_setting->first))
        {
            return possible_prefix_setting->second;
        }
    }

    return {};
}

S3Settings::RequestSettings::RequestSettings(const Settings & settings)
{
    max_single_read_retries = settings.s3_max_single_read_retries;
    min_upload_part_size = settings.s3_min_upload_part_size;
    upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    max_connections = settings.s3_max_connections;
    check_objects_after_upload = settings.s3_check_objects_after_upload;
    max_unexpected_write_error_retries = settings.s3_max_unexpected_write_error_retries;
    max_list_nums = settings.s3_max_list_nums;
    max_timeout_ms = settings.s3_max_request_ms;
    s3_use_parallel_upload = settings.s3_use_parallel_upload;
    s3_parallel_upload_pool_size = settings.s3_parallel_upload_pool_size;
}

void S3Settings::RequestSettings::updateFromSettingsIfEmpty(const Settings & settings)
{
    if (!max_single_read_retries)
        max_single_read_retries = settings.s3_max_single_read_retries;
    if (!min_upload_part_size)
        min_upload_part_size = settings.s3_min_upload_part_size;
    if (!upload_part_size_multiply_factor)
        upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    if (!upload_part_size_multiply_parts_count_threshold)
        upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    if (!max_single_part_upload_size)
        max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    if (!max_connections)
        max_connections = settings.s3_max_connections;
    if (!max_unexpected_write_error_retries)
        max_unexpected_write_error_retries = settings.s3_max_unexpected_write_error_retries;
    if (!max_list_nums)
        max_list_nums = settings.s3_max_list_nums;
    if (!max_timeout_ms)
        max_timeout_ms = settings.s3_max_request_ms;
    if (!check_objects_after_upload)
        check_objects_after_upload = settings.s3_check_objects_after_upload;
    if (!s3_use_parallel_upload)
        s3_use_parallel_upload = settings.s3_use_parallel_upload;
    if (!s3_parallel_upload_pool_size)
        s3_parallel_upload_pool_size = settings.s3_parallel_upload_pool_size;
}

}
