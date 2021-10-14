#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int CANNOT_PARSE_TEXT;
}

class WriteBuffer;
class ReadBuffer;

struct HaMergeTreeLogEntryData
{
    enum Type
    {
        EMPTY, /// Not used.
        GET_PART, /// Get the part from another replica.
        MERGE_PARTS, /// Merge the parts.
        DROP_RANGE, /// Delete the parts in the specified partition in the specified number range.
        CLEAR_RANGE, /// Load the parts that is dumped in high level storage and delete parts in low level storage.
        CLEAR_COLUMN, /// (Deprecated) Drop specific column from specified partition.
        REPLACE_RANGE, /// Drop certain range of partitions and replace them by new ones (not supported)
        MUTATE_PART, /// Apply one or several mutations to the part.
        CLONE_PART, /// Similar to GET_PART, but only the source replica executes it.
        INGEST_PARTITION, /// Replace columns of part from source replica
        REPLACE_PARTITION, /// Enhance replace partition stability, part level atomic, include DROP_RANGE & GET_PART.
        BAD_LOG,
        COMMIT_TRAN, /// Commit transaction log
        ABORT_TRAN, /// Abort transaction log
        MAX = ABORT_TRAN // Max element of enum Type, remember update this after add new element
    };
    constexpr static size_t TypesCount = Type::MAX + 1;

    static String typeToString(Type type)
    {
        switch (type)
        {
            case HaMergeTreeLogEntryData::GET_PART:
                return "GET_PART";
            case HaMergeTreeLogEntryData::MERGE_PARTS:
                return "MERGE_PARTS";
            case HaMergeTreeLogEntryData::DROP_RANGE:
                return "DROP_RANGE";
            case HaMergeTreeLogEntryData::CLEAR_RANGE:
                return "CLEAR_RANGE";
            case HaMergeTreeLogEntryData::CLEAR_COLUMN:
                return "CLEAR_COLUMN";
            case HaMergeTreeLogEntryData::REPLACE_RANGE:
                return "REPLACE_RANGE";
            case HaMergeTreeLogEntryData::MUTATE_PART:
                return "MUTATE_PART";
            case HaMergeTreeLogEntryData::CLONE_PART:
                return "CLONE_PART";
            case HaMergeTreeLogEntryData::INGEST_PARTITION:
                return "INGEST_PARTITION";
            case HaMergeTreeLogEntryData::REPLACE_PARTITION:
                return "REPLACE_PARTITION";
            case HaMergeTreeLogEntryData::BAD_LOG:
                return "BAD_LOG";
            case HaMergeTreeLogEntryData::COMMIT_TRAN:
                return "COMMIT_TRAN";
            case HaMergeTreeLogEntryData::ABORT_TRAN:
                return "ABORT_TRAN";
            default:
                throw Exception("Unknown log entry type: " + DB::toString<int>(type), ErrorCodes::LOGICAL_ERROR);
        }
    }

    String typeToString() const { return typeToString(type); }

    bool shouldSkipOnReplica(const String & replica_name) const
    {
        switch (type)
        {
            case BAD_LOG:
            case CLEAR_COLUMN: /// CLEAR_COLUMN type is deprecated.
                return true;
            case CLONE_PART:
                return source_replica != replica_name;
            case MUTATE_PART:
                return !from_replica.empty() && from_replica != replica_name;
            default:
                return false;
        }
    }

    /// return whether executing the log may change the logical data of the storage.
    /// note that merge parts will not change table's data.
    bool mayChangeStorageData() const
    {
        return (
            type == GET_PART || type == DROP_RANGE || type == CLEAR_RANGE || type == CLEAR_COLUMN || type == REPLACE_RANGE
            || type == MUTATE_PART || type == CLONE_PART || type == INGEST_PARTITION || type == REPLACE_PARTITION);
    }

    bool hasMergeMutateFutureParts() const
    {
        return type == MERGE_PARTS || type == DROP_RANGE || type == CLEAR_RANGE || type == MUTATE_PART;
    }

    /// If true, should skip the log if the new part is covered by or conflicted with committed parts.
    bool willCommitNewPart() const { return type == GET_PART || type == CLONE_PART || type == MERGE_PARTS || type == MUTATE_PART; }

    void checkNewParts() const;
    String formatNewParts() const;

    bool isAlterMutation() const { return type == MUTATE_PART && alter_version != -1; }

    /// serialization

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
    String toString() const;

    String toDebugString() const;

    /// -------- Members below may be serialized --------

    /// Head
    Type type{EMPTY};
    bool is_executed{false};
    UInt64 lsn{0};
    String source_replica; // where the event happens(originally)
    String block_id;

    /// FOR GET_PART / MERGE_PARTS / DROP_RANGE / CLEAR_RANGE / MUTATE_PART
    Strings new_parts;

    /// FOR MERGE_PARTS / MUTATE_PART
    Strings source_parts;

    /// For DROP_RANGE
    bool detach = false;

    /// For CLONE_PART, where clone from
    /// For MUTATE_PART, if set, only the from_replica needs to execute the log
    String from_replica;

    /// StorageType storage_type = StorageType::Local;

    time_t create_time;

    /// For INGEST PARTITION partition COLUMNS col1, col2 [KEY k1, k2] FROM db.table
    Names column_names;
    Names key_names;

    /// Used by Queue
    bool currently_executing{false};

    /// FOR GET_PART insert transaction id and status
    UInt64 transaction_id{0};
    /// TransactionStatus transaction_status{IN_PROGRESS};
    UInt64 transaction_index{0};

    /// FOR GET_PART insert quorum
    size_t quorum{0};

    /// For MUTATE_PART, set when it's an alter mutation
    int alter_version = -1;

    /// -------- Members above may be serialized --------

    UInt8 format_version = 4; // default version changes in case log breaking
    size_t num_tries{0};
    time_t first_attempt_time{0};
    time_t last_attempt_time{0};
    std::exception_ptr last_exception{nullptr};

    /// If executing the log commits new part, record the actual committed part here.
    /// Used by Queue to maintain parts_to_do in mutation status
    String actual_committed_part;
};

struct HaMergeTreeLogEntry : public HaMergeTreeLogEntryData, std::enable_shared_from_this<HaMergeTreeLogEntry>
{
    using Ptr = std::shared_ptr<HaMergeTreeLogEntry>;
    using Vec = std::vector<Ptr>;

    /// Comparison helper struct
    struct LSNLessCompare
    {
        using is_transparent = void;
        bool operator()(const Ptr & lhs, const Ptr & rhs) const { return lhs->lsn < rhs->lsn; }

        bool operator()(const Ptr & e, UInt64 lsn) const { return e->lsn < lsn; }

        bool operator()(UInt64 lsn, const Ptr & e) const { return lsn < e->lsn; }
    };
    static LSNLessCompare lsn_less_compare;

    struct LSNEqualCompare
    {
        using is_transparent = void;
        bool operator()(const Ptr & lhs, const Ptr & rhs) const { return lhs->lsn == rhs->lsn; }

        bool operator()(const Ptr & e, UInt64 lsn) const { return e->lsn == lsn; }

        bool operator()(UInt64 lsn, const Ptr & e) const { return lsn == e->lsn; }
    };
    static LSNEqualCompare lsn_equal_compare;

    bool isReplicaRelated(const String & replica)
    {
        if (replica.empty())
            return false;
        return replica == source_replica || replica == from_replica;
    }
};

using HaMergeTreeLogEntryPtr = std::shared_ptr<HaMergeTreeLogEntry>;
using HaMergeTreeLogEntryVec = std::vector<HaMergeTreeLogEntryPtr>;

///  useful functions
template <class T, class U>
std::vector<typename T::value_type> logSetDifference(const T & left, const U & right)
{
    std::vector<typename T::value_type> res;
    std::set_difference(
        left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(res), HaMergeTreeLogEntry::lsn_less_compare);
    return res;
}

template <class T, class U>
std::vector<typename T::value_type> logSetIntersection(const T & left, const U & right)
{
    std::vector<typename T::value_type> res;
    std::set_intersection(
        left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(res), HaMergeTreeLogEntry::lsn_less_compare);
    return res;
}

std::ostream & operator<<(std::ostream & os, const HaMergeTreeLogEntry & entry);
}
