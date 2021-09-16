#pragma once

#include <Core/Types.h>
#include <time.h>

namespace DB
{
struct GetMutationStatusResponse
{
    String mutation_pointer;

    String mutation_id;     /// requested mutation id
    bool is_found = false;  /// whether the request mutation is found. if not found, the status is unknown
    bool is_done = false;   /// whether the request mutation is finished
    time_t finish_time = 0; /// record the finish time if is_done is true
};

}
