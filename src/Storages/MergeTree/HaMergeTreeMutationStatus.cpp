#include <Storages/MergeTree/HaMergeTreeMutationStatus.h>

#include <boost/algorithm/string/join.hpp>
#include <Common/Exception.h>
#include <IO/WriteHelpers.cpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNFINISHED;
    extern const int LOGICAL_ERROR;
}

void checkMutationStatus(std::optional<HaMergeTreeMutationStatus> & status, const std::set<String> & mutation_ids)
{
    if (mutation_ids.empty())
        throw Exception("Cannot check mutation status because no mutation ids provided", ErrorCodes::LOGICAL_ERROR);

    if (!status)
    {
        throw Exception("Mutation " + toString(*mutation_ids.begin()) + " was killed", ErrorCodes::UNFINISHED);
    }
    else if (!status->is_done && !status->latest_fail_reason.empty())
    {
        throw Exception(
            toString("Exception happened during execution of mutation") + (mutation_ids.size() > 1 ? "s" : "") + " '"
                + boost::algorithm::join(mutation_ids, ", ") + "' with part '" + status->latest_failed_part + " ' reason: '"
                + status->latest_fail_reason
                + "'. This error maybe retryable or not. "
                  "In case of unretryable error, mutation can be killed with KILL MUTATION query",
            ErrorCodes::UNFINISHED);
    }
}

}
