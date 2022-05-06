#include <Storages/MergeTree/MergeTreeBitMapIndexReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Common/escapeForFileName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int DUPLICATE_COLUMN;
extern const int NOT_FOUND_COLUMN_IN_BLOCK;
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
}

MergeTreeBitMapIndexReader::MergeTreeBitMapIndexReader
    (
        const String & path_,
        const BitMapIndexInfoPtr & bitmap_index_info_,
        const MergeTreeIndexGranularity & index_granularity_,
        const MarkRanges & mark_ranges_
    )
    :
    path(path_),
    bitmap_index_info(bitmap_index_info_),
    index_granularity(index_granularity_),
    mark_ranges(mark_ranges_)
{
    // each column need a reader, load the bitmap index reader for each column
    for (auto it = bitmap_index_info->index_names.begin(); it != bitmap_index_info->index_names.end(); ++it)
    {
        if (!valid_reader)
            break;

        try
        {
            for (auto jt = it->second.begin(); jt != it->second.end(); ++jt)
            {
                if (index_name_set.count(*jt))
                    continue;

                String index_name = *jt;
                index_name_set.insert(index_name);
                String column_name = escapeForFileName(index_name);
                String idx_path = path + column_name + AB_IDX_EXTENSION;
                String irk_path = path + column_name + AB_IRK_EXTENSION;
                Poco::File idx_file{idx_path};
                Poco::File irk_file{irk_path};
                if (idx_file.exists() && irk_file.exists())
                {
                    auto index_reader = std::make_shared<BitmapIndexReader>(path, index_name, BitmapIndexMode::ROW);
                    if (index_reader && index_reader->valid())
                        bitmap_index_readers.insert({index_name, index_reader});
                    else
                    {
                        valid_reader = false;
                        break;
                    }
                }
                else
                {
                    valid_reader = false;
                    break;
                }
            }
        }
        catch(...)
        {
            tryLogCurrentException(&Poco::Logger::get("MergeTreeBitMapIndexReader"), __PRETTY_FUNCTION__);
            valid_reader = false;
            break;
        }
    }

    // std::cout<<"size of readers: "<<bitmap_index_readers.size()<<std::endl;
    // std::cout<<"size of set_args: "<<bitmap_index_info->set_args.size()<<std::endl;
    // std::cout<<"size of index_names: "<<bitmap_index_info->index_names.size()<<std::endl;
    if (bitmap_index_readers.empty())
        valid_reader = false;
}


MergeTreeBitMapIndexReader::~MergeTreeBitMapIndexReader() = default;

/**
 * Default behavior:
 * for a set, it contains multiple elements, each element has a bitmap index
 * this function get index for each element and then OR them by a bit-or operation.
 **/
BitmapIndexPtr MergeTreeBitMapIndexReader::getIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitMapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::vector<BitmapIndexPtr> res_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    if (to_type.isUInt8())
        getBaseIndex<UInt8>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt16())
        getBaseIndex<UInt16>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt32())
        getBaseIndex<UInt32>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt64())
        getBaseIndex<UInt64>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt8())
        getBaseIndex<Int8>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt16())
        getBaseIndex<Int16>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt32())
        getBaseIndex<Int32>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt64())
        getBaseIndex<Int64>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isFloat32())
        getBaseIndex<Float32>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isFloat64())
        getBaseIndex<Float64>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isString())
        getBaseIndex<String>(res_indexes, criterion, column_ptr, bitmap_index_reader);

    auto & bitmap = criterion->getIndex();
    for (size_t idx = 0; idx < res_indexes.size(); ++idx)
    {
        bitmap |= res_indexes.at(idx)->getIndex();
    }

    return criterion;
}

/**
 ** Default behavior:
 ** index_names: multiple functions -> single column
 ** set_args: multiple functions -> single set
 ** Thus, we construct a map (function name, bitmap index), each bitmap index is consisted of all element in set in a disjunctive way (OR operation).
 **/
void MergeTreeBitMapIndexReader::getIndexes()
{
    // for each bitmap, load the needed posting list
    for (auto it = bitmap_index_info->index_names.begin(); it != bitmap_index_info->index_names.end(); ++it)
    {
        BitmapIndexPtr inner_index = std::make_shared<IListIndex>();

        auto set_it = bitmap_index_info->set_args.find(it->first);
        if (set_it == bitmap_index_info->set_args.end())
            throw Exception("Cannot find set for column " + it->first, ErrorCodes::LOGICAL_ERROR);

        if (it->second.size() > 1 || set_it->second.size() > 1)
            throw Exception("Default BitMapIndexReader cannot handle a function with multiple column / sets", ErrorCodes::LOGICAL_ERROR);

        if (it->second.size() == 0 || set_it->second.size() == 0)
            throw Exception("Default BitMapIndexReader cannot handle a function with empty column / sets", ErrorCodes::LOGICAL_ERROR);

        auto index_name = it->second.front();
        auto set_ptr = set_it->second.front();

        auto reader_it = bitmap_index_readers.find(index_name);
        if (reader_it == bitmap_index_readers.end())
            throw Exception("No BitmapIndexReader of " + index_name + "has been found", ErrorCodes::LOGICAL_ERROR);

        const BitmapIndexPtr & list_index = getIndex(reader_it->second, set_ptr);
        inner_index->setOriginalRows(list_index->getOriginalRows());
        inner_index->setIndex(list_index->getIndex());

        // (column name, index)
        indexes.emplace(it->first, inner_index);
    }
}

}
