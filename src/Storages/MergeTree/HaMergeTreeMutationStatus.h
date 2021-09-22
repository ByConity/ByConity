#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <map>
#include <time.h>

namespace DB
{

struct HaMergeTreeMutationStatus
{
    String id;
    String query_id; /// Currently only supported by HaMergeTree
    String command;
    time_t create_time = 0;
    std::map<String, Int64> block_numbers;

    /// A number of parts that should be mutated/merged or otherwise moved to Obsolete state for this mutation to complete.
    Int64 parts_to_do = 0;

    /// Parts that should be mutated/merged or otherwise moved to Obsolete state for this mutation to complete.
    Names parts_to_do_names; /// Currently it's only supported by HaMergeTree

    /// If the mutation is done. Note that in case of ReplicatedMergeTree parts_to_do == 0 doesn't imply is_done == true.
    bool is_done = false;
    /// time when is_done is set to true
    time_t finish_time = 0;

    String latest_failed_part;
    time_t latest_fail_time = 0;
    String latest_fail_reason;
};

/// Check mutation status and throw exception in case of error during mutation
/// (latest_fail_reason not empty) or if mutation was killed (status empty
/// optional). mutation_ids passed separately, because status may be empty and
/// we can execute multiple mutations at once
void checkMutationStatus(std::optional<HaMergeTreeMutationStatus> & status, const std::set<String> & mutation_ids);

}
