
#include <Storages/MergeTree/BitEngineDictionary/IBitEngineDictionaryManager.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionary.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Macros.h>
#include <common/scope_guard.h>

namespace DB
{
////////////////////////////// StartOf BitEngineDictionaryManagerBase

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
bool BitEngineDictionaryManagerBase<T>::recodeBitEnginePart(const MergeTreeData::MutableDataPartPtr & part,
                                                            const MergeTreeData & merge_tree_data,
                                                            bool can_skip,
                                                            bool without_lock)
{
    const NamesAndTypesList & columns = part->getColumns();
    NamesAndTypesList recoded_columns;
    // TODO (liuhaoqiang) finish this
//    for (const auto & column : columns)
//    {
//         If the receiving part has no BitEngineEncode columns but MergeTreeData has,
//         it means we get part from other table which did not encode the corresponding
//         column. In this case, we need to recode this part, otherwise the data of this table
//         will not be decoded.
//         if (column.type->isBitEngineEncode() || merge_tree_data.isBitEngineEncodeColumn(column.name))
//         {
//             if (!column.type->isBitEngineEncode())
//                 // const_cast<IDataType *>(column.type.get())->setFlags(TYPE_BITENGINE_ENCODE_FLAG);
//             recoded_columns.push_back(column);
//         }
//    }

    if (recoded_columns.empty())
        return true;

    // const_cast<MergeTreeDataPartWide *>(part.get())->metainfo->is_encoding = true;
    // SCOPE_EXIT({ const_cast<MergeTreeDataPartWide *>(part.get())->metainfo->is_encoding = false; });

    // MergeTreeData::AlterDataPartTransactionPtr transaction;

    // try
    // {
    //     transaction = recodeBitEnginePartInTransaction(part, recoded_columns, merge_tree_data, can_skip, without_lock);
    // }catch(...)
    // {
    //     transaction = nullptr;
    //     throw;
    // }

    // if (transaction)
    //     transaction->commit();

    return true;
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::recodeBitEngineParts(const MergeTreeData::MutableDataPartsVector & parts,
                                                             const MergeTreeData & merge_tree_data,
                                                             bool can_skip,
                                                             bool without_lock)
{
    for (const auto & part : parts)
    {
        recodeBitEnginePart(part, merge_tree_data, can_skip, without_lock);
        flushDict();
    }
}

template <typename T>
void BitEngineDictionaryManagerBase<T>::recodeBitEnginePartsParallel(MergeTreeData::MutableDataPartsVector & parts,
                                                                     const MergeTreeData & merge_tree_data,
                                                                     ContextPtr query_context,
                                                                     bool can_skip)
{
    if (parts.empty())
        return;

    std::mutex recode_mutex;
    auto data_parts = parts;

    ThreadGroupStatusPtr thread_group = CurrentThread::getGroup();

    auto runRecodeBitEnginePart = [&]()
    {
        setThreadName("RecodeBitEnginePart");
        CurrentThread::attachToIfDetached(thread_group);
        while (1)
        {
            MergeTreeData::MutableDataPartPtr part;
            {
                std::lock_guard<std::mutex> lock(recode_mutex);
                if (!data_parts.empty())
                {
                    part = data_parts.back();
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
                recodeBitEnginePart(part, merge_tree_data, can_skip, query_context->getSettingsRef().bitengine_encode_without_lock);
            }
            catch(...){
                throw;
            }
        }
    };

    size_t max_threads = query_context->getSettingsRef().max_parallel_threads_for_bitengine_recode;
    size_t num_threads = std::min(max_threads, data_parts.size());
    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
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
