#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <queue>
#include <mutex>
#include <iostream>

namespace DB
{

class QueueForAsyncTask
{
public:
    using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
    std::deque<DataPartPtr> part_queue;
    std::mutex part_mutex;

    bool push(const DataPartPtr & part)
    {
        // TODO dongyifeng add it later
//        if (part->info.storage_type != StorageType::Local)
//            return false;
        std::lock_guard<std::mutex> lock(part_mutex);
        for (const auto & in_part : part_queue)
        {
            if (in_part == part || in_part->getFullPath() == part->getFullPath())
                return false;
        }
        part_queue.push_back(part);
        return true;
    }

    DataPartPtr pop()
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        if (part_queue.empty())
            return nullptr;
        DataPartPtr part = part_queue.front();
        part_queue.pop_front();
        return part;
    }

    bool empty()
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        return part_queue.empty();
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        return part_queue.size();
    }

    void clear()
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        part_queue.clear();
    }

    Names getParts()
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        Names names;
        names.reserve(part_queue.size());
        for (auto & part : part_queue)
            names.push_back(part->name);
        return names;
    }

    virtual String toString()
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        std::stringstream ss;
        ss << "Size of Async Queue: " << part_queue.size() << "\n";
        ss << "Elements of Async Queue: \n";
        for (const auto & in_part : part_queue)
            ss << in_part->name << "\n";
        return ss.str();
    }

    virtual ~QueueForAsyncTask() {}
};

class QueueForIndex : public QueueForAsyncTask
{
public:
    String toString() override
    {
        std::lock_guard<std::mutex> lock(part_mutex);
        std::stringstream ss;
        ss << "Size of bitmap: " << part_queue.size() << "\n";
        ss << "Elements of bitmap: \n";
        for (const auto & in_part : part_queue)
            ss << in_part->name << "\n";
        return ss.str();
    }
};

}
