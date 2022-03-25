#include <Storages/MergeTree/HaMergeTreeLogEntry.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <common/LocalDateTime.h>

namespace DB
{
HaMergeTreeLogEntry::LSNLessCompare lsn_less_compare;
HaMergeTreeLogEntry::LSNEqualCompare lsn_equal_compare;

String HaMergeTreeLogEntryData::typeToString(Type t)
{
    switch (t)
    {
        case HaMergeTreeLogEntryData::GET_PART:
            return "GET_PART";
        case HaMergeTreeLogEntryData::MERGE_PARTS:
            return "MERGE_PARTS";
        case HaMergeTreeLogEntryData::DROP_RANGE:
            return "DROP_RANGE";
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
        case HaMergeTreeLogEntryData::BAD_LOG:
            return "BAD_LOG";
        case HaMergeTreeLogEntryData::COMMIT_TRAN:
            return "COMMIT_TRAN";
        case HaMergeTreeLogEntryData::ABORT_TRAN:
            return "ABORT_TRAN";
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown log entry type {}", int(t));
    }
}

void HaMergeTreeLogEntryData::writeText(WriteBuffer & out) const
{
    Int32 status_int;
    out << "format version: ha/" << format_version << "\n"
        << "create_time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "is_executed: " << is_executed << '\n'
        << "lsn: " << lsn << '\n'
        << "source replica: " << source_replica << '\n'
        << "block_id: " << escape << block_id << '\n';

    String storage_type_str = "local";
    switch (type)
    {
        case GET_PART:
            status_int = 0;
            out << "get\n" << new_part_name;
            if (format_version >= 2)
            {
                out << "\n";
                out << "xid\n" << transaction_id << "\n";
                out << "index\n" << transaction_index << "\n";
                out << "status\n" << status_int;
            }
            if (format_version >= 3)
            {
                out << "\n";
                out << "storage_type\n" << storage_type_str;
            }
            if (format_version >= 4)
            {
                out << "\n";
                out << "quorum\n" << quorum;
            }
            break;

        case MERGE_PARTS:
            out << "merge\n";
            for (auto & s : source_parts)
                out << s << '\n';
            out << "into\n" << new_part_name;
            break;
        case DROP_RANGE:
            if (detach)
                out << "detach\n";
            else
                out << "drop\n";
            out << new_part_name;
            break;

        case CLEAR_COLUMN:
            out << "clear_column\n" << escape << column_names.front() << "\nfrom\n" << new_part_name;
            break;
        case REPLACE_RANGE:
            out << "replace_range\n";
            out << "drop_range_name: " << replace_range_entry->drop_range_part_name << "\n";
            out << "new_parts: ";
            writeQuoted(replace_range_entry->new_part_names, out);
            break;
        case MUTATE_PART:
            out << "mutate2\n"
                << source_parts.at(0) << "\n"
                << "to\n"
                << new_part_name << "\n"
                << "alter_version\n"
                << alter_version << "\n"
                << "from_replica\n"
                << from_replica;
            break;

        case CLONE_PART:
            out << "clone\n" << new_part_name << "\n";
            out << "from\n" << from_replica;
            if (format_version >= 3)
            {
                out << "\n";
                out << "storage_type\n" << storage_type_str;
            }
            break;

        case INGEST_PARTITION:
            out << "ingest\n";
            if (format_version >= 5)
            {
                out << "source_database " << source_database << "\n";
                out << "source_table " << source_table << "\n";
            }
            out << new_part_name << "\n";
            out << "columns " << column_names.size();
            for (auto & s : column_names)
                out << "\n" << s;
            out << "\nkeys " << key_names.size();
            for (auto & s : key_names)
                out << "\n" << s;
            break;

        case BAD_LOG:
            out << "bad\n";
            break;

        case COMMIT_TRAN:
            status_int = 0;
            out << "commit\n";
            out << "xid\n" << transaction_id << "\n";
            out << "index\n" << transaction_index << "\n";
            out << "status\n" << status_int;
            break;

        case ABORT_TRAN:
            status_int = 0;
            out << "abort\n";
            out << "xid\n" << transaction_id << "\n";
            out << "index\n" << transaction_index << "\n";
            out << "status\n" << status_int;
            break;

        default:
            throw Exception("Unknown log entry type: " + DB::toString<int>(type), ErrorCodes::LOGICAL_ERROR);
    }

    out << '\n';
}

void HaMergeTreeLogEntryData::readText(ReadBuffer & in)
{
    String type_str;
    Int32 status_int;
    String storage_type_str;

    in >> "format version: ha/" >> format_version >> "\n";
    if (format_version < 1 || format_version > 5)
        throw Exception("Unknown HaMergeTreeLogEntry format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

    LocalDateTime create_time_dt;
    in >> "create_time: " >> create_time_dt >> "\n";
    create_time = DateLUT::instance().makeDateTime(
        create_time_dt.year(),
        create_time_dt.month(),
        create_time_dt.day(),
        create_time_dt.hour(),
        create_time_dt.minute(),
        create_time_dt.second());
    in >> "is_executed: " >> is_executed >> "\n";
    in >> "lsn: " >> lsn >> "\n";
    in >> "source replica: " >> source_replica >> "\n";
    in >> "block_id: " >> escape >> block_id >> "\n";

    in >> type_str >> "\n";
    if (type_str == "get")
    {
        type = GET_PART;
        in >> new_part_name;
        if (format_version >= 2)
        {
            in >> "\n";
            in >> "xid\n" >> transaction_id >> "\n";
            in >> "index\n" >> transaction_index >> "\n";
            in >> "status\n" >> status_int;
            /// transaction_status = TransactionStatus(status_int);
        }
        if (format_version >= 3)
        {
            in >> "\n";
            in >> "storage_type\n" >> storage_type_str;
        }
        if (format_version >= 4)
        {
            in >> "\n";
            in >> "quorum\n" >> quorum;
        }
    }
    else if (type_str == "merge")
    {
        type = MERGE_PARTS;
        while (true)
        {
            String s;
            in >> s >> "\n";
            if (s == "into")
                break;
            source_parts.push_back(std::move(s));
        }
        in >> new_part_name;
    }
    else if (type_str == "replace_range")
    {
        type = REPLACE_RANGE;
        replace_range_entry = std::make_shared<ReplaceRangeEntry>();

        in >> "drop_range_name: " >> replace_range_entry->drop_range_part_name >> "\n";
        in >> "new_parts: ";
        readQuoted(replace_range_entry->new_part_names, in);
    }
    else if (type_str == "replace") /// XXX: remove me
    {
        type = REPLACE_RANGE;
        replace_range_entry = std::make_shared<ReplaceRangeEntry>();

        size_t new_parts_size = 1;
        while (true)
        {
            String s;
            in >> s;

            if (startsWith(s, "into"))
            {
                const size_t INTO_LEN = strlen("into ");
                if (s.length() > INTO_LEN)
                    new_parts_size = parse<UInt64>(s.substr(INTO_LEN));
                break;
            }
            else
            {
                in >> "\n";
            }
            replace_range_entry->drop_range_part_name = std::move(s);
        }
        for (size_t i = 0; i < new_parts_size; ++i)
        {
            replace_range_entry->new_part_names.emplace_back();
            in >> "\n" >> replace_range_entry->new_part_names.back();
        }
    }
    else if (type_str == "drop" || type_str == "detach")
    {
        type = DROP_RANGE;
        detach = (type_str == "detach");
        in >> new_part_name;
    }
    else if (type_str == "mutate" || type_str == "mutate2")
    {
        type = MUTATE_PART;
        source_parts.emplace_back();
        in >> source_parts.back() >> "\n" >> "to\n" >> new_part_name;
        if (type_str == "mutate2")
        {
            in >> "\nalter_version\n" >> alter_version >> "\nfrom_replica\n" >> from_replica;
        }
    }
    else if (type_str == "clone")
    {
        type = CLONE_PART;
        in >> new_part_name >> "\n";
        in >> "from\n" >> from_replica;
        if (format_version >= 3)
        {
            in >> "\n";
            in >> "storage_type\n" >> storage_type_str;
            /// if (storage_type_str == "local")
            ///     storage_type = StorageType::Local;
            /// else
            ///     storage_type = StorageType::Hdfs;
        }
    }
    else if (type_str == "clear_column")
    {
        type = CLEAR_COLUMN;
        column_names.emplace_back();
        in >> escape >> column_names.back() >> "\nfrom\n" >> new_part_name;
    }
    else if (type_str == "ingest")
    {
        type = INGEST_PARTITION;
        if (format_version >= 5)
        {
            in >> "source_database " >> source_database >> "\n";
            in >> "source_table " >> source_table >> "\n"; 
        }
        in >> new_part_name >> "\n";

        size_t column_size;
        String column_str;
        in >> column_str;
        const size_t COLUMN_LEN = strlen("columns ");
        column_size = parse<UInt64>(column_str.substr(COLUMN_LEN));

        for (size_t i = 0; i < column_size; ++i)
        {
            column_names.emplace_back();
            in >> "\n" >> column_names.back();
        }

        size_t key_size;
        String key_str;
        in >> "\n" >> key_str;
        const size_t KEY_LEN = strlen("keys ");
        key_size = parse<UInt64>(key_str.substr(KEY_LEN));

        for (size_t i = 0; i < key_size; ++i)
        {
            key_names.emplace_back();
            in >> "\n" >> key_names.back();
        }
    }
    else if (type_str == "bad")
    {
        type = BAD_LOG;
    }
    else if (type_str == "commit")
    {
        type = COMMIT_TRAN;
        in >> "xid\n" >> transaction_id >> "\n";
        in >> "index\n" >> transaction_index >> "\n";
        in >> "status\n" >> status_int;
        /// transaction_status = TransactionStatus(status_int);
    }
    else if (type_str == "abort")
    {
        type = ABORT_TRAN;
        in >> "xid\n" >> transaction_id >> "\n";
        in >> "index\n" >> transaction_index >> "\n";
        in >> "status\n" >> status_int;
        /// transaction_status = TransactionStatus(status_int);
    }
    else
    {
        throw Exception("Unknown Log Entry Type" + type_str, ErrorCodes::LOGICAL_ERROR);
    }

    in >> "\n";
}

String HaMergeTreeLogEntryData::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

String HaMergeTreeLogEntryData::toDebugString() const
{
    std::ostringstream oss;
    oss << "{Log-" << lsn << ", src " << source_replica << ", " << typeToString(type) << ' ';

    switch (type)
    {
        case GET_PART:
        case CLONE_PART:
        case DROP_RANGE:
            oss << new_part_name;
            break;
        case MERGE_PARTS:
        case MUTATE_PART:
            for (auto & old_part : source_parts)
                oss << old_part << ' ';
            oss << "-> " << new_part_name;
            break;
        case REPLACE_RANGE:
            oss << replace_range_entry->drop_range_part_name << " with";
            for (auto & new_part : replace_range_entry->new_part_names)
                oss << " " << new_part;
            break;
        default:
            oss << "N/A";
            break;
    }
    oss << '}';
    return oss.str();
}

}
