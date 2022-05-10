#pragma once
#include <cstddef>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{

class MMappedFileCache;
using MMappedFileCachePtr = std::shared_ptr<MMappedFileCache>;


struct MergeTreeReaderSettings
{
    size_t min_bytes_to_use_direct_io = 0;
    size_t min_bytes_to_use_mmap_io = 0;
    MMappedFileCachePtr mmap_cache;
    size_t max_read_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    /// If save_marks_in_cache is false, then, if marks are not in cache,
    ///  we will load them but won't save in the cache, to avoid evicting other data.
    bool save_marks_in_cache = false;
    /// Convert old-style nested (single arrays with same prefix, `n.a`, `n.b`...) to subcolumns of data type Nested.
    bool convert_nested_to_subcolumns = false;
    /// Validate checksums on reading (should be always enabled in production).
    bool checksum_on_read = true;
    /// used by BitEngine to read user-input but not encoded bitmaps in part
    bool read_source_bitmap = false;
};

struct BitEngineEncodeSettings
{
    BitEngineEncodeSettings() = default;
    BitEngineEncodeSettings(const Settings & global_settings,
                            const MergeTreeSettingsPtr & storage_settings)
        : without_lock(global_settings.bitengine_encode_without_lock)
        , encode_fast_mode(global_settings.bitengine_encode_fast_mode)
        , loss_rate(storage_settings->bitengine_encode_loss_rate)
    {}

    Float64 getLossRate() const { return loss_rate; }

    BitEngineEncodeSettings & bitengineOnlyRecode(bool value = false)
    {
        only_recode = value;
        return *this;
    }

    BitEngineEncodeSettings & bitengineEncodeWithoutLock(bool value = false)
    {
        without_lock = value;
        return *this;
    }

    BitEngineEncodeSettings & bitengineEncodeFastMode(bool value = false)
    {
        encode_fast_mode = value;
        return *this;
    }

    bool only_recode = false;
    bool without_lock = false;
    bool encode_fast_mode = false;
    bool skip_bitengine_encode = false;

private:
    Float64 loss_rate=0.1;
};

struct MergeTreeWriterSettings
{
    MergeTreeWriterSettings() = default;

    MergeTreeWriterSettings(
        const Settings & global_settings,
        const MergeTreeSettingsPtr & storage_settings,
        bool can_use_adaptive_granularity_,
        bool rewrite_primary_key_,
        bool blocks_are_granules_size_ = false,
        bool skip_bitengine_encode_ = false,
        bool optimize_map_column_serialization_ = false)
        : min_compress_block_size(
            storage_settings->min_compress_block_size ? storage_settings->min_compress_block_size : global_settings.min_compress_block_size)
        , max_compress_block_size(
              storage_settings->max_compress_block_size ? storage_settings->max_compress_block_size
                                                        : global_settings.max_compress_block_size)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , rewrite_primary_key(rewrite_primary_key_)
        , blocks_are_granules_size(blocks_are_granules_size_)
        , bitengine_settings(global_settings, storage_settings)
        , optimize_map_column_serialization(optimize_map_column_serialization_)
    {
        bitengine_settings.skip_bitengine_encode = skip_bitengine_encode_;
    }

    size_t min_compress_block_size;
    size_t max_compress_block_size;
    bool can_use_adaptive_granularity;
    bool rewrite_primary_key;
    bool blocks_are_granules_size;

    BitEngineEncodeSettings bitengine_settings;
    bool optimize_map_column_serialization = false;
};

}
