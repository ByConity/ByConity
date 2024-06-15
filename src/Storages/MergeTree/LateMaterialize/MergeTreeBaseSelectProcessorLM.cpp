#include <memory>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/Exception.h>

namespace ProfileEvents
{
    extern const Event PrewhereSelectedRows;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_BITMAP_INDEX_READER;
}

void MergeTreeBaseSelectProcessorLM::prepareBitMapIndexReader(const MergeTreeIndexContextPtr & index_context, const NameSet & bitmap_columns, MergeTreeIndexExecutorPtr & executor)
{
    if (bitmap_columns.empty() || index_context == nullptr)
    {
        executor = nullptr;
        return;
    }
    executor = index_context ? index_context->getIndexExecutor(
                   task->data_part,
                   task->data_part->index_granularity,
                   storage.getSettings()->bitmap_index_segment_granularity,
                   storage.getSettings()->bitmap_index_serializing_granularity,
                   task->mark_ranges_total_read) : nullptr;

    if (executor && executor->valid())
        executor->initReader(MergeTreeIndexInfo::Type::BITMAP, bitmap_columns);
    else
    {
        throw Exception(
            "Need to read bitmap index columns, but bitmap index reader is invalid. "
            "Maybe memory limit exceeded, try again later",
            ErrorCodes::INVALID_BITMAP_INDEX_READER);
    }
}

// add mark ranges into segment bitmap grabed from the pool
void MergeTreeBaseSelectProcessorLM::insertMarkRangesForSegmentBitMapIndexFunctions(
    MergeTreeIndexExecutorPtr & index_executor, const MarkRanges & mark_ranges_inc)
{
    if (!index_executor)
        return;

    auto bitmap_index_reader = index_executor->getReader(MergeTreeIndexInfo::Type::BITMAP);
    if (!bitmap_index_reader || !bitmap_index_reader->validIndexReader())
        return;

    bitmap_index_reader->addSegmentIndexesFromMarkRanges(mark_ranges_inc);
}

MergeTreeBaseSelectProcessorLM::MergeTreeBaseSelectProcessorLM(
    Block header,
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const SelectQueryInfo & query_info_,
    const MergeTreeStreamSettings & stream_settings_,
    const Names & virt_column_names_)
    : SourceWithProgress(transformHeader(
        std::move(header), query_info_, storage_.getPartitionValueType(), virt_column_names_))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , stream_settings(stream_settings_)
    , virt_column_names(virt_column_names_)
    , partition_value_type(storage.getPartitionValueType())
{
    header_without_virtual_columns = getPort().getHeader();
    for (auto it = virt_column_names.rbegin(); it != virt_column_names.rend(); ++it)
        if (header_without_virtual_columns.has(*it))
            header_without_virtual_columns.erase(*it);

    const auto & atomic_predicates = getAtomicPredicates(query_info_);
    for (const auto & p : atomic_predicates)
    {
        if (!p)
        {
            atomic_predicate_list.emplace_back(nullptr);
            continue;
        }
        auto & expr = atomic_predicate_list.emplace_back(std::make_shared<AtomicPredicateExpr>());
        if (p->predicate_actions)
            expr->predicate_actions = std::make_shared<ExpressionActions>(p->predicate_actions, stream_settings.actions_settings);
        expr->filter_column_name = p->filter_column_name;
        if (p->index_context)
            expr->index_context = p->index_context;
        expr->is_row_filter = p->is_row_filter;
        expr->remove_filter_column = p->remove_filter_column;
        if (expr->index_context)
        {
            auto * bitmap_index_info = dynamic_cast<BitmapIndexInfo *>(expr->index_context->get(MergeTreeIndexInfo::Type::BITMAP).get());
            if (bitmap_index_info)
            {
                const auto & name_set = bitmap_index_info->index_column_name_set;
                bitmap_index_columns_super_set.insert(name_set.begin(), name_set.end());
            }
        }
    }
    /// If query don't have where after moving, then MUST filter before returning chunk
    need_filter = query_info_.query->as<ASTSelectQuery &>().where() == nullptr;
}

Chunk MergeTreeBaseSelectProcessorLM::generate()
{
    while (!isCancelled())
    {
        if ((!task || task->isFinished()) && !getNewTask())
            return {};

        auto res = readFromPart();

        if (res.hasRows())
        {
            MergeTreeBaseSelectProcessor::injectVirtualColumns(res, task.get(), partition_value_type, virt_column_names);
            return res;
        }
    }

    return {};
}

/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeBaseSelectProcessorLM::getNewTask()
{
    if (!getNewTaskImpl())
        return false;

    /// There're no task at all, but there're some pre-computed chunks somewhere
    /// that we can return. In that case just return true as a signal that can continue
    /// reading from this source
    if (!task)
        return true;

    const std::string part_name = task->data_part->isProjectionPart() ? task->data_part->getParentPart()->name : task->data_part->name;

    if (readers.empty())
    {
        size_t num_stages = atomic_predicate_list.size();
        readers.resize(num_stages);
        index_executors.resize(num_stages);
        if (stream_settings.use_uncompressed_cache)
            owned_uncompressed_cache = storage.getContext()->getUncompressedCache();
        owned_mark_cache = storage.getContext()->getMarkCache();
        const auto & task_columns = task->task_columns;
        for (size_t i = 0; i < atomic_predicate_list.size(); ++i)
        {
            prepareBitMapIndexReader(atomic_predicate_list[i]->index_context, task_columns.per_stage_bitmap_nameset[i], index_executors[i]);
            readers[i] = task->data_part->getReader(
                task_columns.per_stage_columns[i],
                storage_snapshot->metadata,
                task->mark_ranges_total_read,
                owned_uncompressed_cache.get(),
                owned_mark_cache.get(),
                stream_settings.reader_settings,
                index_executors[i].get(),
                IMergeTreeReader::ValueSizeMap{},
                profile_callback);
        }
        chain_ready = false;
    }
    else
    {
        /// in other case we can reuse readers, anyway they will be "seeked" to required mark
        if (part_name != last_readed_part_name)
        {
            const auto & task_columns = task->task_columns;
            for (size_t i = 0; i < atomic_predicate_list.size(); ++i)
            {
                prepareBitMapIndexReader(atomic_predicate_list[i]->index_context, task_columns.per_stage_bitmap_nameset[i], index_executors[i]);
                readers[i] = task->data_part->getReader(
                    task_columns.per_stage_columns[i],
                    storage_snapshot->metadata,
                    task->mark_ranges_total_read,
                    owned_uncompressed_cache.get(),
                    owned_mark_cache.get(),
                    stream_settings.reader_settings,
                    index_executors[i].get(),
                    IMergeTreeReader::ValueSizeMap{},
                    profile_callback);
            }
            chain_ready = false;
        }
        else
        {
            // segment bitmap need adding mark ranges to the reader of the same part
            for (size_t i = 0; i < atomic_predicate_list.size(); ++i)
            {
                if (readers[i]->hasBitmapIndexReader())
                    insertMarkRangesForSegmentBitMapIndexFunctions(index_executors[i], task->mark_ranges_once_read);
            }
        }
    }
    last_readed_part_name = part_name;
    return true;
}


void MergeTreeBaseSelectProcessorLM::initializeChain()
{
    updateGranuleCounter();
    MergeTreeRangeReaderLM * prev_range_reader = nullptr;
    range_readers.clear();
    range_readers.resize(readers.size());
    for (auto i = readers.size() - 1; i > 0; --i)
    {
        /// Only need delete bitmap for the first reader in chain
        ImmutableDeleteBitmapPtr delete_bitmap = (i == readers.size() - 1) ? task->delete_bitmap : nullptr;
        range_readers[i] = MergeTreeRangeReaderLM(
            readers[i].get(), prev_range_reader, atomic_predicate_list[i].get(), delete_bitmap, false);
        prev_range_reader = &range_readers[i];
    }

    /// if the last reader doesn't read anything then skip it;
    if (readers[0]->getColumns().empty() && (!atomic_predicate_list[0] || !(atomic_predicate_list[0]->predicate_actions)) && readers.size() > 1)
        range_readers[1].makeLastReader();
    else
    {
        /// If chain size > 1, previous reader already handle the delete bitmap; otherwise we need to
        /// handle it ourself
        ImmutableDeleteBitmapPtr delete_bitmap = readers.size() == 1 ? task->delete_bitmap : nullptr;
        range_readers[0]
            = MergeTreeRangeReaderLM(readers[0].get(), prev_range_reader, atomic_predicate_list[0].get(), delete_bitmap, true);
    }

    chain_ready = true;
}

void MergeTreeBaseSelectProcessorLM::initializeTaskReader()
{
    if (readers[0]->getColumns().empty() && (!atomic_predicate_list[0] || !(atomic_predicate_list[0]->predicate_actions)) && readers.size() > 1)
        task->msr_range_reader = &range_readers[1];
    else
        task->msr_range_reader = &range_readers[0];
}

Chunk MergeTreeBaseSelectProcessorLM::readFromPartImpl()
{
    /// TODO: use block size predictor
    ReadResult read_result = task->msr_range_reader->read(task->mark_ranges_once_read, need_filter);

    const auto & sample_block = task->msr_range_reader->getSampleBlock();
    // fmt::print("sample block {}\n", sample_block.dumpStructure());
    // fmt::print("Header {}\n", header_without_virtual_columns.dumpStructure());
    // fmt::print("Num rows {}\n", read_result.num_rows);
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception(
            "Inconsistent number of columns got from MergeTreeRangeReader. "
            "Have "
                + toString(sample_block.columns())
                + " in sample block "
                  "and "
                + toString(read_result.columns.size()) + " columns in list",
            ErrorCodes::LOGICAL_ERROR);

    progress({read_result.numReadRows(), read_result.numBytesRead()});
    ProfileEvents::increment(ProfileEvents::PrewhereSelectedRows, read_result.num_rows);

    if (read_result.num_rows == 0) /// read nothing
        return {};

    Columns ordered_columns;
    ordered_columns.reserve(header_without_virtual_columns.columns());

    /// Reorder columns. TODO: maybe skip for default case.
    for (size_t ps = 0; ps < header_without_virtual_columns.columns(); ++ps)
    {
        const auto & name = header_without_virtual_columns.getByPosition(ps).name;
        if (sample_block.has(name))
        {
            auto pos_in_sample_block = sample_block.getPositionByName(name);
            ordered_columns.emplace_back(std::move(read_result.columns[pos_in_sample_block]));
            if (read_result.bitmap_block.has(name))
                read_result.bitmap_block.erase(name);
        }
        else if (!bitmap_index_columns_super_set.contains(name))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find columns " + name + " in result block");
        }
        else
        {
            ordered_columns.emplace_back(header_without_virtual_columns.getByPosition(ps).type->createColumnConstWithDefaultValue(read_result.num_rows));
        }
    }

    auto chunk = Chunk(std::move(ordered_columns), read_result.num_rows);
    /// When this function is call, sure task is not null
    for (auto && col : read_result.bitmap_block)
        chunk.addColumnToSideBlock(std::move(col));

    // if (chunk.getSideBlock())
    //     fmt::print("Chunk side block {}\n", chunk.getSideBlock()->dumpStructure());
    return chunk;
}

Chunk MergeTreeBaseSelectProcessorLM::readFromPart()
{
    if (!chain_ready)
        initializeChain();
    if (!task->msr_range_reader)
        initializeTaskReader();
    return readFromPartImpl();
}

bool MergeTreeBaseSelectProcessorLM::getNewTaskImpl()
{
    throw Exception("Not Implemented", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeBaseSelectProcessorLM::~MergeTreeBaseSelectProcessorLM() = default;

Block MergeTreeBaseSelectProcessorLM::transformHeader(
    Block block, const SelectQueryInfo & query_info, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    /// TODO: may be we need to handle the chain here later on.
    const auto & predicates = query_info.atomic_predicates;
    for (auto it = predicates.rbegin(); it != predicates.rend(); ++it)
    {
        const auto & p = *it;
        if (p && p->predicate_actions)
        {
            block = p->predicate_actions->updateHeader(std::move(block));
            if (p->remove_filter_column)
                block.erase(p->filter_column_name);
        }
    }
    // fmt::print(stderr, "Header after read: {}\n", block.dumpNames());
    MergeTreeBaseSelectProcessor::injectVirtualColumns(block, nullptr, partition_value_type, virtual_columns);
    return block;
}

}

