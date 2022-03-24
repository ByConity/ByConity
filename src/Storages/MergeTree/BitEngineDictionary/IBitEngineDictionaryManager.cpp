
#include <Storages/MergeTree/BitEngineDictionary/IBitEngineDictionaryManager.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionary.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Macros.h>
#include <common/scope_guard.h>
#include <common/logger_useful.h>

namespace DB
{
////////////////////////////// StartOf BitEngineDictionaryManagerBase
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

template <typename T>
BitEngineDictionaryManagerBase<T>::BitEngineDictionaryManagerBase(const String & db_tbl_, const String & disk_name_, const String & dict_path_, ContextPtr context_)
    : IBitEngineDictionaryManager(db_tbl_, disk_name_, dict_path_, context_), dict_containers()
{
    String shard_index_str;
    try{
        shard_index_str = context->getMacros()->expand(shard_id_macro);
    }catch(...){
        // do nothing
        //tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
    if (shard_index_str.empty())
        shard_id = 0;
    else
        shard_id = parse<UInt64>(shard_index_str);
}


template <typename T>
void BitEngineDictionaryManagerBase<T>::lightFlush()
{
    WriteLock lock = getWriteLock();
    for (auto & item : dict_containers)
    {
        if (item.second)
            item.second->lightFlushDict();
    }
}

template <typename T>
bool BitEngineDictionaryManagerBase<T>::updated()
{
    bool updated = false;
    /// update all dicts
    for (auto & item : dict_containers)
    {
        if (item.second && item.second->updated())
            updated = true;
    }
    return updated;
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::close()
{
    auto lock = getWriteLock();
    for (auto & item : dict_containers)
    {
        if (item.second)
            item.second->close();
    }
}


template <typename T>
void BitEngineDictionaryManagerBase<T>::rename(const String & new_db_tbl, const String & new_dict_path)
{
    auto lock = getWriteLock();
    db_tbl = new_db_tbl;
    path = new_dict_path;
    for (auto & item : dict_containers)
    {
        if (item.second)
            item.second->renameTo(new_db_tbl, new_dict_path);
    }
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::drop()
{
    auto lock = getWriteLock();
    for (auto & item : dict_containers)
    {
        if (item.second)
            item.second->drop();
    }
    dict_containers.clear();
    dropped = true;
}

template <typename T>
bool BitEngineDictionaryManagerBase<T>::isValid()
{
    bool is_valid = true;
    for (auto & item : dict_containers)
    {
        if (item.second)
        {
            is_valid &= item.second->isValid();
        }
    }
    return is_valid;
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::setValid()
{
    for (auto & item : dict_containers)
    {
        if (item.second)
        {
            item.second->setValid();
        }
    }
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::setInvalid()
{
    for (auto & item : dict_containers)
    {
        if (item.second)
        {
            item.second->setInvalid();
        }
    }
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::checkBitEnginePart(const MergeTreeData::DataPartPtr & part) const
{
    const NamesAndTypesList & columns = part->getColumns();
    NamesAndTypesList check_columns;
    for (const auto & column : columns)
    {
        if (isBitmap64(column.type))
            check_columns.push_back(column);
    }

    if (check_columns.empty())
        throw Exception("BitEngine manager cannot find a column with BitEngineEncode flag", ErrorCodes::LOGICAL_ERROR);

    for (const auto & column : check_columns)
    {
        String file_path = part->getFullPath() + column.name + BITENGINE_DATA_FILE_EXTENSION;
        Poco::File bitengine_file(file_path);
        if (!bitengine_file.exists())
            throw Exception("Column " + column.name + " in part " + part->name + " should recode by bitengine", ErrorCodes::LOGICAL_ERROR);
    }
}


template <typename T>
bool BitEngineDictionaryManagerBase<T>::recodeBitEnginePart(const FutureMergedMutatedPart & future_part,
                                                            const MergeTreeData & merge_tree_data,
                                                            ContextPtr query_context,
                                                            bool can_skip,
                                                            bool part_in_detach)
{
    Stopwatch watcher;
    if (future_part.parts.size() != 1)
        throw Exception("Trying to encoding " + toString(future_part.parts.size()) + " parts, not one in recodeBitEnginePart function. "
                        "This is a bug.", ErrorCodes::LOGICAL_ERROR);

    const NamesAndTypesList & columns = future_part.parts.at(0)->getColumns();
    NamesAndTypesList recoded_columns;
    for (const auto & column : columns)
    {
//         If the receiving part has no BitEngineEncode columns but MergeTreeData has,
//         it means we get part from other table which did not encode the corresponding
//         column. In this case, we need to recode this part, otherwise the data of this table
//         will not be decoded.
         if (column.type->isBitEngineEncode() || merge_tree_data.isBitEngineEncodeColumn(column.name))
         {
             if (!column.type->isBitEngineEncode())
                  const_cast<IDataType *>(column.type.get())->setFlags(TYPE_BITENGINE_ENCODE_FLAG);
             recoded_columns.push_back(column);
         }
    }

    if (recoded_columns.empty())
        return true;

    const_cast<IMergeTreeDataPart *>(future_part.parts.at(0).get())->is_encoding = true;
    SCOPE_EXIT({ const_cast<IMergeTreeDataPart *>(future_part.parts.at(0).get())->is_encoding = false; });

    /// if we encode part, then we should reserve space on the same disk, because mutations possible can create hardlinks
    ReservationPtr reserved_space = merge_tree_data.tryReserveSpace(MergeTreeDataMergerMutator::estimateNeededDiskSpace({future_part.parts.at(0)}),
                                                                     future_part.parts.at(0)->volume);
    if (!reserved_space)
        throw Exception("Not enough space for encoding part '" + future_part.parts.at(0)->name + "' ", ErrorCodes::NOT_ENOUGH_SPACE);

    const_cast<FutureMergedMutatedPart &>(future_part).updatePath(merge_tree_data, reserved_space);

    const auto & source_part = future_part.parts.at(0);
    String source_part_name  = source_part->name;
    size_t source_part_rows = source_part->rows_count;
    bool without_lock = query_context->getSettingsRef().bitengine_encode_without_lock;
    try
    {
        auto space_reservation = merge_tree_data.tryReserveSpace(MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part.parts), future_part.parts.at(0)->volume);
        auto new_part = encodePartToTemporaryPart(future_part, recoded_columns, merge_tree_data, space_reservation, can_skip, part_in_detach, without_lock);
        if (new_part)
        {
            if (part_in_detach)
                const_cast<MergeTreeData &>(merge_tree_data).renameTempPartInDetachDirecotry(new_part, source_part);
            else
                const_cast<MergeTreeData &>(merge_tree_data).renameTempPartAndReplace(new_part);
        }
    }
    catch(...)
    {
        throw;
    }

    UInt64 elapsed_ms = watcher.elapsedMilliseconds();
    double elapsed_s = watcher.elapsedSeconds();
    String time_str = elapsed_ms < 10*1000*1000000UL ? std::to_string(elapsed_ms)+" ms" : std::to_string(elapsed_s)+" s";
    LOG_DEBUG(&Poco::Logger::get("BitEngineDictionaryManager"), "BitEngine encode part {} containing {} rows cost {}", source_part_name, source_part_rows, time_str);

    return true;
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::recodeBitEngineParts(
    const std::vector<FutureMergedMutatedPart> & future_parts,
    const MergeTreeData & merge_tree_data,
    ContextPtr query_context,
    bool can_skip,
    bool part_in_detach)
{
    for (const auto & part : future_parts)
    {
        recodeBitEnginePart(part, merge_tree_data, query_context, can_skip, part_in_detach);
    }
    flushDict();
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::recodeBitEnginePartsParallel(
    const std::vector<FutureMergedMutatedPart> & future_parts,
    const MergeTreeData & merge_tree_data,
    ContextPtr query_context,
    bool can_skip,
    bool part_in_detach)
{
    if (future_parts.empty())
        return;

    std::mutex recode_mutex;
    auto data_parts = future_parts;

    ThreadGroupStatusPtr thread_group = CurrentThread::getGroup();

    auto runRecodeBitEnginePart = [&]()
    {
        setThreadName("ParaEncBtEngPt");
        CurrentThread::attachToIfDetached(thread_group);
        while (true)
        {
            FutureMergedMutatedPart * part{nullptr};
            {
                std::lock_guard<std::mutex> lock(recode_mutex);
                if (!data_parts.empty())
                {
                    part = &(data_parts.back());
                    data_parts.pop_back();
                }
                else
                {
                    break;
                }
            }

            if (!part)
                return;

            try{
                recodeBitEnginePart(*part, merge_tree_data, query_context, can_skip, part_in_detach);
            }
            catch(...){
                throw;
            }
        }
    };

    size_t max_threads = query_context->getSettingsRef().max_parallel_threads_for_bitengine_recode;
    size_t num_threads = std::min(max_threads, data_parts.size());
    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
    LOG_DEBUG(&Poco::Logger::get("BitEngineDictionaryManager"), "BitEngine will encode {} parts In Parallel with {} threads",
              future_parts.size(), num_threads);

    for (size_t i = 0; i<num_threads; i++)
    {
        thread_pool->scheduleOrThrowOnError(runRecodeBitEnginePart);
    }

    thread_pool->wait();

    // only flush at the end of bitengine recode
    flushDict();
}



template class BitEngineDictionaryManagerBase<BitEngineDictionaryPtr>;
}
