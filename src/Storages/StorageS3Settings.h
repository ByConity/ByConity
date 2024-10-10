#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

#include <IO/S3Common.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

struct Settings;

struct S3Settings
{
    struct RequestSettings
    {
        size_t max_single_read_retries = 0;
        size_t min_upload_part_size = 0;
        size_t upload_part_size_multiply_factor = 0;
        size_t upload_part_size_multiply_parts_count_threshold = 0;
        size_t max_single_part_upload_size = 0;
        size_t max_connections = 0;
        bool check_objects_after_upload = false;
        size_t max_unexpected_write_error_retries = 0;
        UInt64 max_list_nums = 1000;
        UInt64 max_timeout_ms = 30000;
        bool s3_use_parallel_upload = false;
        size_t s3_parallel_upload_pool_size = 0;

        bool throw_on_zero_files_match = false;

        RequestSettings() = default;
        explicit RequestSettings(const Settings & settings);

        inline bool operator==(const RequestSettings & other) const
        {
            return max_single_read_retries == other.max_single_read_retries && min_upload_part_size == other.min_upload_part_size
                && upload_part_size_multiply_factor == other.upload_part_size_multiply_factor
                && upload_part_size_multiply_parts_count_threshold == other.upload_part_size_multiply_parts_count_threshold
                && max_single_part_upload_size == other.max_single_part_upload_size && max_connections == other.max_connections
                && check_objects_after_upload == other.check_objects_after_upload
                && max_unexpected_write_error_retries == other.max_unexpected_write_error_retries && max_list_nums == other.max_list_nums
                && max_timeout_ms == other.max_timeout_ms && s3_use_parallel_upload == other.s3_use_parallel_upload
                && s3_parallel_upload_pool_size == other.s3_parallel_upload_pool_size;
        }

        void updateFromSettingsIfEmpty(const Settings & settings);
    };

    String endpoint;
    S3::AuthSettings auth_settings;
    RequestSettings request_settings;

    inline bool operator==(const S3Settings & other) const
    {
        return endpoint == other.endpoint && auth_settings == other.auth_settings && request_settings == other.request_settings;
    }
};

/// Settings for the StorageS3.
class StorageS3Settings
{
public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings);

    S3Settings getSettings(const String & endpoint) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const S3Settings> s3_settings;
};

}
