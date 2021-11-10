#include <Storages/MergeTree/HaMergeTreeAltersSequence.h>

#include <cassert>
#include <iostream>
#include <Storages/MutationCommands.h>
#include <common/logger_useful.h>

namespace DB
{


namespace
{
    auto collectColumnsFromCommands(const MutationCommands & commands)
    {
        std::set<std::string> res;
        for (auto & command : commands)
            res.insert(command.column_name);
        return res;
    }

    void mergeFrom(std::set<std::string> & A, const std::set<std::string> & B)
    {
        for (auto & v : B)
            A.insert(v);
    }

    void eraseFrom(std::set<std::string> & A, const std::set<std::string> & B)
    {
        for (auto it = A.begin(); it != A.end();)
        {
            if (B.count(*it))
                it = A.erase(it);
            else
                ++it;
        }
    }
}


int HaMergeTreeAltersSequence::getHeadAlterVersion(std::lock_guard<std::mutex> & /*state_lock*/) const
{
    /// If queue empty, than we don't have version
    if (!queue_state.empty())
        return queue_state.begin()->first;
    return -1;
}

bool HaMergeTreeAltersSequence::hasUnfinishedMetadataAlter(std::lock_guard<std::mutex> & /*state_lock*/) const
{
    for (auto & [_, state] : queue_state)
        if (!state.metadata_finished)
            return true;
    return false;
}

bool HaMergeTreeAltersSequence::hasUnfinishedDataAlter(std::lock_guard<std::mutex> & /*state_lock*/) const
{
    for (auto & [_, state] : queue_state)
        if (!state.data_finished)
            return true;
    return false;
}

std::optional<std::string>
HaMergeTreeAltersSequence::canAddMetadataAlter(const MutationCommands & commands, std::lock_guard<std::mutex> & /*state_lock*/) const
{
    for (auto & command : commands)
    {
        /// If current_columns contains a column which will be modified by the commands, the commands cannot be added
        if (!command.column_name.empty() && current_columns.count(command.column_name))
            return std::make_optional(command.column_name);
    }
    return std::nullopt;
}

void HaMergeTreeAltersSequence::addMutationForAlter(int alter_version, const MutationCommands & commands, std::lock_guard<std::mutex> & /*state_lock*/)
{
    /// Metadata alter must be added before, but
    /// maybe already finished if we startup after metadata alter was finished.
    if (!queue_state.count(alter_version))
    {
        auto columns = collectColumnsFromCommands(commands);
        mergeFrom(current_columns, columns);
        queue_state.try_emplace(alter_version, /*metadata_finished*/ true, /*data_finished*/ false, /*have_mutation*/ true, std::move(columns));
    }
    else
        queue_state[alter_version].data_finished = false;
}

void HaMergeTreeAltersSequence::addMetadataAlter(int alter_version, const MutationCommands & commands, std::lock_guard<std::mutex> & /*state_lock*/)
{
    /// Metadata alter is always added before. See HaMergeTreeQueue::pullLogsToQueue.
    if (!queue_state.count(alter_version))
    {
        auto columns = collectColumnsFromCommands(commands);
        mergeFrom(current_columns, columns);
        queue_state.try_emplace(alter_version, /*metadata_finished*/ false, /*data_finished*/ true, commands.willMutateData(), std::move(columns));
    }
    else
    {
        queue_state[alter_version].metadata_finished = false;
        LOG_WARNING(&Poco::Logger::get(__PRETTY_FUNCTION__), "There is already existed data alter. This should not happen.");
    }
}

void HaMergeTreeAltersSequence::finishMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
{
    /// Sequence must not be empty
    assert(!queue_state.empty());

    /// If metadata stage finished (or was never added) than we can remove this alter
    if (queue_state[alter_version].data_finished)
    {
        eraseFrom(current_columns, queue_state[alter_version].columns);
        queue_state.erase(alter_version);
    }
    else
        queue_state[alter_version].metadata_finished = true;
}

void HaMergeTreeAltersSequence::finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
{
    /// Queue can be empty after load of finished mutation without move of mutation pointer
    if (queue_state.empty())
        return;

    /// Mutations may finish multiple times (for example, after server restart, before update of mutation pointer)
    if (alter_version >= queue_state.begin()->first)
    {
        /// All alter versions bigger than head must present in queue.
        assert(queue_state.count(alter_version));

        if (queue_state[alter_version].metadata_finished)
        {
            eraseFrom(current_columns, queue_state[alter_version].columns);
            queue_state.erase(alter_version);
        }
        else
            queue_state[alter_version].data_finished = true;
    }
}

bool HaMergeTreeAltersSequence::canExecuteMetaAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
{
    assert(!queue_state.empty());

    if (!queue_state.at(alter_version).have_mutations)
        return true;

    for (auto & [version, state] : queue_state)
    {
        if (version < alter_version)
        {
            if (!state.data_finished) /// There must not be an unfinished data mutation
                return false;
        }
        else if (version == alter_version)
            return true;
        else // >
            return false;
    }

    return false;
}

bool HaMergeTreeAltersSequence::canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
{
    /// Queue maybe empty when we start after server shutdown
    /// and have some MUTATE_PART records in queue
    if (queue_state.empty())
        return true;

    /// All versions smaller than head, can be executed
    if (alter_version < queue_state.begin()->first)
        return true;

    return queue_state.at(alter_version).metadata_finished;
}

}
