#include <Storages/MergeTree/HaMergeTreeLogEntry.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>


namespace DB
{
HaMergeTreeLogEntry::LSNLessCompare lsn_less_compare;
HaMergeTreeLogEntry::LSNEqualCompare lsn_equal_compare;

void HaMergeTreeLogEntryData::checkNewParts() const
{
    switch (type)
    {
        case GET_PART:
        case MERGE_PARTS:
        case DROP_RANGE:
        case CLEAR_RANGE:
        case MUTATE_PART:
        case CLONE_PART:
            if (new_parts.empty())
                throw Exception("No new part for " + typeToString(), ErrorCodes::LOGICAL_ERROR);
            break;
        default:
            break;
    }
}

String HaMergeTreeLogEntryData::formatNewParts() const
{
    if (new_parts.empty())
        return "";
    String res = new_parts.front();
    for (size_t i = 1; i < new_parts.size(); ++i)
    {
        res += " & ";
        res += new_parts[i];
    }
    return res;
}

void HaMergeTreeLogEntryData::writeText(WriteBuffer & out) const
{
    checkNewParts();
    Int32 status_int;
    out << "format version: ha/" << format_version << "\n"
        << "create_time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "is_executed: " << is_executed << '\n'
        << "lsn: " << lsn << '\n'
        << "source replica: " << source_replica << '\n'
        << "block_id: " << escape << block_id << '\n';

    /// String storage_type_str = (storage_type == StorageType::Local ? "local" : "hdfs");
    String storage_type_str = "local";
    switch (type)
    {
        case GET_PART:
            status_int = 0;
            out << "get\n" << new_parts.front();
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
            if (new_parts.size() > 1)
            {
                out << "into " << new_parts.size();
                for (auto & s : new_parts)
                    out << '\n' << s;
            }
            else
            {
                out << "into\n" << new_parts.front();
            }
            break;
        case REPLACE_PARTITION:
            out << "replace\n";
            out << source_parts.front() << '\n';
            if (new_parts.size() > 1)
            {
                out << "into " << new_parts.size();
                for (auto & s : new_parts)
                    out << '\n' << s;
            }
            else
            {
                out << "into\n" << new_parts.front();
            }
            break;
        case CLEAR_RANGE:
            out << "clear\n";
            out << new_parts.front();
            break;
        case DROP_RANGE:
            if (detach)
                out << "detach\n";
            else
                out << "drop\n";
            out << new_parts.front();
            break;

        case CLEAR_COLUMN:
            out << "clear_column\n" << escape << column_names.front() << "\nfrom\n" << new_parts.front();
            break;
        case REPLACE_RANGE:
            throw Exception("Unsupported Ha log entry type: " + typeToString(type), ErrorCodes::LOGICAL_ERROR);
        case MUTATE_PART:
            out << "mutate2\n"
                << source_parts.at(0) << "\n"
                << "to\n"
                << new_parts.front() << "\n"
                << "alter_version\n"
                << alter_version << "\n"
                << "from_replica\n"
                << from_replica;
            break;

        case CLONE_PART:
            out << "clone\n" << new_parts.front() << "\n";
            out << "from\n" << from_replica;
            if (format_version >= 3)
            {
                out << "\n";
                out << "storage_type\n" << storage_type_str;
            }
            break;

        case INGEST_PARTITION:
            out << "ingest\n";
            out << new_parts.front() << "\n";
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
    if (format_version != 1 && format_version != 2 && format_version != 3 && format_version != 4)
        throw Exception("Unknown HaMergeTreeLogEntry format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

    LocalDateTime create_time_dt;
    in >> "create_time: " >> create_time_dt >> "\n";
    create_time = create_time_dt;
    in >> "is_executed: " >> is_executed >> "\n";
    in >> "lsn: " >> lsn >> "\n";
    in >> "source replica: " >> source_replica >> "\n";
    in >> "block_id: " >> escape >> block_id >> "\n";

    in >> type_str >> "\n";
    if (type_str == "get")
    {
        type = GET_PART;
        /// storage_type = StorageType::Local;
        new_parts.emplace_back();
        in >> new_parts.back();
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
            /// if (storage_type_str == "local")
            ///     storage_type = StorageType::Local;
            /// else
            ///     storage_type = StorageType::Hdfs;
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
            source_parts.push_back(std::move(s));
        }
        for (size_t i = 0; i < new_parts_size; ++i)
        {
            new_parts.emplace_back();
            in >> "\n" >> new_parts.back();
        }
    }
    else if (type_str == "replace")
    {
        type = REPLACE_PARTITION;

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
            source_parts.push_back(std::move(s));
        }
        for (size_t i = 0; i < new_parts_size; ++i)
        {
            new_parts.emplace_back();
            in >> "\n" >> new_parts.back();
        }
    }
    else if (type_str == "clear")
    {
        type = CLEAR_RANGE;
        detach = false;
        new_parts.emplace_back();
        in >> new_parts.back();
    }
    else if (type_str == "drop" || type_str == "detach")
    {
        type = DROP_RANGE;
        detach = (type_str == "detach");
        new_parts.emplace_back();
        in >> new_parts.back();
    }
    else if (type_str == "mutate" || type_str == "mutate2")
    {
        type = MUTATE_PART;
        source_parts.emplace_back();
        new_parts.emplace_back();
        in >> source_parts.back() >> "\n" >> "to\n" >> new_parts.back();
        if (type_str == "mutate2")
        {
            in >> "\nalter_version\n" >> alter_version >> "\nfrom_replica\n" >> from_replica;
        }
    }
    else if (type_str == "clone")
    {
        type = CLONE_PART;
        new_parts.emplace_back();
        in >> new_parts.back() >> "\n";
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
        new_parts.emplace_back();
        in >> escape >> column_names.back() >> "\nfrom\n" >> new_parts.back();
    }
    else if (type_str == "ingest")
    {
        type = INGEST_PARTITION;
        new_parts.emplace_back();
        in >> new_parts.back() >> "\n";

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

    checkNewParts();
}

String HaMergeTreeLogEntryData::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

/// Compacted information in one line, for debugging
std::ostream & operator<<(std::ostream & os, const HaMergeTreeLogEntry & entry)
{
    os << "<LSN " << entry.lsn << '|' << "REP " << entry.source_replica;
    switch (entry.type)
    {
        case HaMergeTreeLogEntry::BAD_LOG:
            os << "|BAD";
            break;
        case HaMergeTreeLogEntry::GET_PART:
            os << "|GET " << entry.new_parts.front();
            break;
        case HaMergeTreeLogEntry::CLONE_PART:
            os << "|CLONE " << entry.new_parts.front();
            break;
        case HaMergeTreeLogEntry::MERGE_PARTS:
            os << "|MERGE ";
            for (auto & old_part : entry.source_parts)
                os << old_part << ' ';
            os << "-> ";
            for (auto & new_part : entry.new_parts)
                os << new_part << ' ';
            break;
        case HaMergeTreeLogEntry::DROP_RANGE:
            os << "|DROP " << entry.new_parts.front();
            break;
        case HaMergeTreeLogEntry::MUTATE_PART:
            os << "|MUTATE " << entry.source_parts.front() << " -> " << entry.new_parts.front();
            break;
        case HaMergeTreeLogEntry::REPLACE_PARTITION:
            os << "|REPLACE " << entry.source_parts.front() << "-> ";
            for (auto & new_part : entry.new_parts)
                os << new_part << ' ';
            break;
        /// TODO: support more types
        default:
            os << "| " << HaMergeTreeLogEntry::typeToString(entry.type);
    }
    os << '>';
    return os;
}

}
