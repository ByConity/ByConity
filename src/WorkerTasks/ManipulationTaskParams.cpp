#include <WorkerTasks/ManipulationTaskParams.h>

#include <sstream>
#include <Parsers/formatAST.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOG_ERROR;
}

String ManipulationTaskParams::toDebugString() const
{
    std::ostringstream oss;

    oss << "ManipulationTask {" << task_id << "}, type "  << typeToString(type) << ", ";
    oss << storage->getStorageID().getNameForLogs() << ": ";
    if (!source_data_parts.empty())
    {
        oss << " source_parts: { " << source_data_parts.front()->name;
        for (size_t i = 1; i < source_data_parts.size(); ++i)
            oss << ", " << source_data_parts[i]->name;
        oss << " }";
    }
    if (!new_part_names.empty())
    {
        oss << " new_part_names: { " << new_part_names.front();
        for (size_t i = 1; i < new_part_names.size(); ++i)
            oss << ", " << new_part_names[i];
        oss << " }";
    }
    if (mutation_commands)
    {
        oss << " mutation_commands: " << serializeAST(*mutation_commands->ast());
    }

    if (columns_commit_time)
        oss << " columns_commit_time: " << columns_commit_time;

    return oss.str();
}

void ManipulationTaskParams::assignSourceParts(MergeTreeDataPartsVector parts)
{
    if (unlikely(type == Type::Empty))
        throw Exception("Expected non-empty manipulate type", ErrorCodes::LOGICAL_ERROR);

    if (parts.empty())
        return;

    auto left = parts.begin();
    auto right = parts.begin();

    while (left != parts.end())
    {
        if (type == ManipulationType::Merge)
        {
            while (right != parts.end() && (*left)->partition.value == (*right)->partition.value)
                ++right;
        }
        else
        {
            ++right;
        }

        MergeTreePartInfo part_info;
        part_info.partition_id = (*left)->info.partition_id;
        part_info.min_block = (*left)->info.min_block;
        part_info.max_block = (*std::prev(right))->info.max_block;
        part_info.level = (*left)->info.level + 1;

        // TODO: Double check any issue: previously the mutation is set to max part's mutation, now set mutation to current txn id.
        // part_info.mutation = (*std::prev(right))->info.mutation;
        part_info.mutation = txn_id;

        for (auto it = left; it != right; ++it)
        {
            part_info.level = std::max(part_info.level, (*it)->info.level + 1);
        }

        if ((*left)->storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            DayNum min_date = DayNum(std::numeric_limits<UInt16>::max());
            DayNum max_date = DayNum(std::numeric_limits<UInt16>::min());
            for (auto it = left; it != right; ++it)
            {
                /// NOTE: getting min and max dates from part names (instead of part data) because we want
                /// the merged part name be determined only by source part names.
                /// It is simpler this way when the real min and max dates for the block range can change
                /// (e.g. after an ALTER DELETE command).
                DayNum part_min_date;
                DayNum part_max_date;
                MergeTreePartInfo::parseMinMaxDatesFromPartName((*it)->name, part_min_date, part_max_date);
                min_date = std::min(min_date, part_min_date);
                max_date = std::max(max_date, part_max_date);
            }

            new_part_names.push_back(part_info.getPartNameV0(min_date, max_date));
        }
        else
            new_part_names.push_back(part_info.getPartName());

        left = right;
    }

    source_data_parts = std::move(parts);
}

}
