#include <Storages/MergeTree/HaMergeTreeMutationEntry.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/Exception.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <common/LocalDateTime.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool HaMergeTreeMutationEntry::duplicateWith(const HaMergeTreeMutationEntry & rhs) const
{
    if (query_id != rhs.query_id)
        return false;

    if (commands.size() == rhs.commands.size())
    {
        WriteBufferFromOwnString out;
        WriteBufferFromOwnString out2;
        commands.writeText(out);
        rhs.commands.writeText(out2);
        if (out.str() == out2.str())
            return true;
    }
    throw Exception("Found mutation " + rhs.znode_name + " with the same query id but different commands", ErrorCodes::BAD_ARGUMENTS);
}

bool HaMergeTreeMutationEntry::extractPartitionIds(MergeTreeData & storage, ContextPtr context)
{
    if (commands.size() == 1)
    {
        MutationCommand & command = commands[0];
        switch (command.type)
        {
            case MutationCommand::FAST_DELETE:
                if (command.partition)
                {
                    partition_ids.emplace(storage.getPartitionIDFromQuery(command.partition, context));
                }
                break;
            case MutationCommand::DROP_COLUMN:
                if (command.partition)
                {
                    if (command.clear)
                    {
                        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
                        /// MergeTreeData::DataPartsVector clear_parts = storage.getPartsByPredicate(command.partition);
                        /// /// No part will apply this mutation, just ignore and return.
                        /// if (clear_parts.empty())
                        ///     return false;
                        /// for (auto & part : clear_parts)
                        /// {
                        ///     partition_ids.emplace(part->info.partition_id);
                        /// }
                    }
                    else
                    {
                        partition_ids.emplace(storage.getPartitionIDFromQuery(command.partition, context));
                    }
                }
                break;
            default:
                break;
        }
    }
    return true;
}

bool HaMergeTreeMutationEntry::coverPartitionId(const String & partition_id) const
{
    return partition_ids.empty() || partition_ids.count(partition_id);
}

void HaMergeTreeMutationEntry::writeText(WriteBuffer & out) const
{
    out << "format version: 2\n"
        << "query id: " << query_id << "\n"
        << "create time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "source replica: " << source_replica << "\n"
        << "block number: " << block_number << "\n"
        << "alter_version: " << alter_version << "\n";

    if (partition_ids.size() == 1)
    {
        out << "partition_id:\n" << *(partition_ids.begin()) << "\n";
    }
    else
    {
        out << "partition_ids:\n" << partition_ids.size() << "\n";
        for (auto & partition_id : partition_ids)
        {
            out << partition_id << "\n";
        }
    }

    if (!isAlterMetadata())
    {
        out << "commands:\n";
        commands.writeText(out);
    }
    else
    {
        out << "alter_info:\n"
            << "have_mutation: " << int(alter_info->have_mutation) << "\n"
            << "columns_str: " << alter_info->columns_str.size() << "\n"
            << alter_info->columns_str << "\n"
            << "metadata_str: " << alter_info->metadata_str.size() << "\n"
            << alter_info->metadata_str << "\n";

        if (commands.empty())
        {
            out << "no_commands:\n";
        }
        else
        {
            out << "commands:\n";
            commands.writeText(out);
        }
    }
}

void HaMergeTreeMutationEntry::readText(ReadBuffer & in)
{
    UInt32 version = 1;
    in >> "format version: " >> version >> "\n";
    in >> "query id: " >> query_id >> "\n";

    LocalDateTime create_time_dt;
    in >> "create time: " >> create_time_dt >> "\n";
    create_time = DateLUT::instance().makeDateTime(
        create_time_dt.year(),
        create_time_dt.month(),
        create_time_dt.day(),
        create_time_dt.hour(),
        create_time_dt.minute(),
        create_time_dt.second());

    in >> "source replica: " >> source_replica >> "\n";
    in >> "block number: " >> block_number >> "\n";
    in >> "alter_version: " >> alter_version >> "\n";

    String type;
    in >> type;
    if (type == "partition_id:")
    {
        String partition_id;
        in >> "\n" >> partition_id >> "\n";
        partition_ids.emplace(partition_id);
        in >> type;
    }

    if (type == "partition_ids:")
    {
        size_t size;
        in >> "\n" >> size >> "\n";
        for (size_t i = 0 ; i < size; i++)
        {
            String partition_id;
            in >> partition_id >> "\n";
            partition_ids.emplace(partition_id);
        }
        in >> type;
    }

    if (type == "commands:")
    {
        in >> "\n";
        commands.readText(in);
    }
    else if (type == "alter_info:")
    {
        in >> "\n";

        makeAlterMetadata();
        in >> "have_mutation: " >> alter_info->have_mutation >> "\n";

        size_t columns_str_size = 0;
        in >> "columns_str: " >> columns_str_size >> "\n";
        alter_info->columns_str.resize(columns_str_size);
        in.readStrict(&alter_info->columns_str[0], columns_str_size);
        in >> "\n";

        size_t metadata_str_size = 0;
        in >> "metadata_str: " >> metadata_str_size >> "\n";
        alter_info->metadata_str.resize(metadata_str_size);
        in.readStrict(&alter_info->metadata_str[0], metadata_str_size);
        in >> "\n";

        if (version >= 2)
        {
            String has_commands;
            in >> has_commands >> "\n";
            if (has_commands == "commands:")
                commands.readText(in);
        }
    }
    else
    {
        throw Exception("Unknown mutation type: " + type, ErrorCodes::LOGICAL_ERROR);
    }
}

String HaMergeTreeMutationEntry::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

String HaMergeTreeMutationEntry::getNameForLogs() const
{
    if (alter_version == -1)
        return "Mutation{znode=" + znode_name + ", query_id=" + query_id + "}";
    else if (alter_info)
        return "AlterMetadata{znode=" + znode_name + ", alter_version=" + DB::toString(alter_version) + "}";
    else
        return "AlterData{znode=" + znode_name + ", alter_version=" + DB::toString(alter_version) + "}";
}

HaMergeTreeMutationEntry HaMergeTreeMutationEntry::parse(const String & str, String znode_name)
{
    HaMergeTreeMutationEntry res;
    res.znode_name = std::move(znode_name);

    ReadBufferFromString in(str);
    res.readText(in);
    assertEOF(in);
    return res;
}

}
