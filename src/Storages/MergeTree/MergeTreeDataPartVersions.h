#pragma once
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <map>

namespace DB
{
/**
 * In order to consider compatibility, data part needs to adopt different strategies for the new
 * and old data after making some implementation improvements. Therefore, this class can be used
 * to record different versions of the implementation, which is convenient for judging in the code.
 */
struct MergeTreeDataPartVersions
{
    bool enable_compact_map_data = false;

    MergeTreeDataPartVersions(bool enable_compact_map_data_): enable_compact_map_data(enable_compact_map_data_) {}

    MergeTreeDataPartVersions(const MergeTreeSettingsPtr & settings) : enable_compact_map_data(settings->enable_compact_map_data) { }

    void write(WriteBuffer & to);

    bool read(ReadBuffer & from, bool needCheckHeader = true);
};

}
