#pragma once

#include <map>
#include <mutex>
#include <set>
#include <string>

namespace DB
{
class MutationCommands;

/// ALTERs in StorageHaMergeTree have to be executed sequentially (one
/// by one). But HaMergeTreeQueue execute all entries almost
/// concurrently. The only dependency between entries is data parts, but they are
/// not suitable in alters case.
///
/// This class stores information about current alters in
/// HaMergeTreeQueue, and control their order of execution. Actually
/// it's a part of HaMergeTreeQueue and shouldn't be used directly by
/// other classes, also methods have to be called under HaMergeTreeQueue
/// state lock.
class HaMergeTreeAltersSequence
{
private:
    /// In general case alter consist of two stages Alter data and alter
    /// metadata. First we alter storage metadata and then we can apply
    /// corresponding data changes (MUTATE_PART). After that, we can remove
    /// alter from this sequence (alter is processed).
    struct AlterState
    {
        bool metadata_finished = false;
        bool data_finished = false;
        bool have_mutations = false;
        std::set<std::string> columns;

        AlterState() = default;
        AlterState(bool m, bool d, bool h, std::set<std::string> c)
            : metadata_finished(m), data_finished(d), have_mutations(h), columns(std::move(c))
        {
        }
    };

private:
    /// alter_version -> AlterState.
    std::map<int, AlterState> queue_state;
    /// The variable collects all the related columns which the queued alters will modify (ADD/DROP/MODIFY)
    /// It is used for detecting column conflicts
    std::set<std::string> current_columns;

public:
    bool hasUnfinishedMetadataAlter(std::lock_guard<std::mutex> & /*state_lock*/) const;
    bool hasUnfinishedDataAlter(std::lock_guard<std::mutex> & /*state_lock*/) const;

    /// Check whether there are column conflicts
    /// @return the first conflicted column
    std::optional<std::string> canAddMetadataAlter(const MutationCommands & commands, std::lock_guard<std::mutex> & /*state_lock*/) const;

    /// Add mutation for alter (alter data stage).
    void addMutationForAlter(int alter_version, const MutationCommands & commands, std::lock_guard<std::mutex> & /*state_lock*/);

    /// Add metadata for alter (alter metadata stage).
    void addMetadataAlter(int alter_version, const MutationCommands & commands, std::lock_guard<std::mutex> & /*state_lock*/);

    /// Finish metadata alter. If corresponding data alter finished, than we can remove
    /// alter from sequence.
    void finishMetadataAlter(int alter_version, std::lock_guard <std::mutex> & /*state_lock*/);

    /// Finish data alter. If corresponding metadata alter finished, than we can remove
    /// alter from sequence.
    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/);

    /// Check that we can execute this data alter. If it's metadata stage finished.
    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const;

    /// Check that we can execute metadata alter with version.
    bool canExecuteMetaAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const;

    /// Just returns smallest alter version in sequence (first entry)
    int getHeadAlterVersion(std::lock_guard<std::mutex> & /*state_lock*/) const;
};

}
