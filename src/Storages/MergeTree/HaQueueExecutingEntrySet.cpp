#include <Storages/MergeTree/HaQueueExecutingEntrySet.h>

#include <Storages/MergeTree/HaMergeTreeQueue.h>
#include <cassert>

namespace DB
{
HaQueueExecutingEntrySet::~HaQueueExecutingEntrySet()
{
    if (!executing && remove_on_success.empty())
        return;

    std::lock_guard queue_lock(queue.state_mutex);
    if (executing)
        executing->currently_executing = false;
    for (auto & e : remove_on_success)
        e->currently_executing = false;
}

void HaQueueExecutingEntrySet::setExecuting(const LogEntryPtr & e)
{
    e->currently_executing = true;
    e->num_tries += 1;
    e->last_attempt_time = time(nullptr);
    if (0 == e->first_attempt_time)
        e->first_attempt_time = e->last_attempt_time;
    executing = e;
}

void HaQueueExecutingEntrySet::merge(HaQueueExecutingEntrySet && rhs)
{
    remove_on_success.insert(std::move(rhs.executing));
    remove_on_success.merge(std::move(rhs.remove_on_success));
}

HaMergeTreeLogEntry::Vec HaQueueExecutingEntrySet::collect() const
{
    LogEntry::Vec res(remove_on_success.begin(), remove_on_success.end());
    if (executing)
        res.push_back(executing);
    return res;
}

std::unique_ptr<FetchingPartToExecutingEntrySet::Handle>
FetchingPartToExecutingEntrySet::insertOrMerge(const String & part_name, const HaQueueExecutingEntrySetPtr & executing_set)
{
    assert(executing_set != nullptr);
    std::unique_lock lock(m);
    auto res = part_to_entry_set.try_emplace(part_name, executing_set);

    if (res.second)
    {
        return std::make_unique<Handle>(*this, res.first);
    }
    else
    {
        auto & exists_executing_set = res.first->second;
        exists_executing_set->merge(std::move(*executing_set));
        return nullptr;
    }
}

}
