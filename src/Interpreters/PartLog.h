#pragma once

#include <unordered_map>
#include <Interpreters/SystemLog.h>
#include "common/types.h"


namespace DB
{

struct PartLogElement
{
    enum Type
    {
        NEW_PART = 1,
        MERGE_PARTS = 2,
        DOWNLOAD_PART = 3,
        REMOVE_PART = 4,
        MUTATE_PART = 5,
        MOVE_PART = 6,
        PRELOAD_PART = 7,
        DROPCACHE_PART = 8,
    };

    String query_id;

    Type event_type = NEW_PART;

    time_t start_time = 0;
    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;
    UInt64 duration_ms = 0;

    String database_name;
    String table_name;
    String part_name;
    String partition_id;
    String partition;
    String path_on_disk;

    /// Size of the part
    UInt64 rows = 0;
    UInt64 segments_count = 0;
    std::unordered_map<String, UInt64> segments;
    UInt64 preload_level = 0;

    /// Size of files in filesystem
    UInt64 bytes_compressed_on_disk = 0;

    /// Makes sense for merges and mutations.
    Strings source_part_names;
    UInt64 bytes_uncompressed = 0;
    UInt64 rows_read = 0;
    UInt64 bytes_read_uncompressed = 0;
    UInt64 peak_memory_usage = 0;

    /// Was the operation successful?
    UInt16 error = 0;
    String exception;


    static std::string name() { return "PartLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class IMergeTreeDataPart;


/// Instead of typedef - to allow forward declaration.
class PartLog : public SystemLog<PartLogElement>
{
    using SystemLog<PartLogElement>::SystemLog;

    using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;

public:
    /// Add a record about creation of new part.
    static bool addNewPart(ContextPtr context, const MutableDataPartPtr & part, UInt64 elapsed_ns,
                           const ExecutionStatus & execution_status = {});
    static bool addNewParts(ContextPtr context, const MutableDataPartsVector & parts, UInt64 elapsed_ns,
                            const ExecutionStatus & execution_status = {});
    static PartLogElement createElement(PartLogElement::Type event_type, const IMergeTreeDataPartPtr & part,
                            UInt64 elapsed_ns = 0, const String & exception = "", UInt64 submit_ts = 0, UInt64 segments_count = 0, std::unordered_map<String, UInt64> segments = {}, UInt64 preload_level = 0);
};

}
