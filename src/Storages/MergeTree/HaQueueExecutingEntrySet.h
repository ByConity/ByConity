#pragma once

#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <map>

namespace DB
{
class HaMergeTreeQueue;

class HaQueueExecutingEntrySet
{
public:
    using LogEntry = HaMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;

    HaQueueExecutingEntrySet(HaMergeTreeQueue & q) : queue(q) {}
    ~HaQueueExecutingEntrySet();

    void setExecuting(const LogEntryPtr & e);
    auto & getExecuting() { return executing; }

    bool valid() const { return executing != nullptr; }
    void merge(HaQueueExecutingEntrySet && s);
    LogEntry::Vec collect() const;

private:
    HaMergeTreeQueue & queue;
    LogEntryPtr executing;
    std::set<LogEntryPtr, LogEntry::LSNLessCompare> remove_on_success;
};

using HaQueueExecutingEntrySetPtr = std::shared_ptr<HaQueueExecutingEntrySet>;

class FetchingPartToExecutingEntrySet
{
    friend class Handle;
public:
    using PartToEntrySet = std::map<String, HaQueueExecutingEntrySetPtr>;

    class Handle
    {
    public:
        Handle(FetchingPartToExecutingEntrySet & p, PartToEntrySet::iterator i) : parent(p), it(i) { }

        auto & getExecutingLog() { return it->second->getExecuting(); }

        ~Handle()
        {
            std::lock_guard lock(parent.m);
            parent.part_to_entry_set.erase(it);
        }

    private:
        FetchingPartToExecutingEntrySet & parent;
        PartToEntrySet::iterator it;
    };

    /// Thread safe; assume only one new part
    std::unique_ptr<Handle> insertOrMerge(const String & part_name, const HaQueueExecutingEntrySetPtr & executing_set);

private:
    std::mutex m;
    PartToEntrySet part_to_entry_set;
};

}
