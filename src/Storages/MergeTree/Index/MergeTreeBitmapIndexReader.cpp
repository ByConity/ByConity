#include <iterator>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Poco/Logger.h>
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

const size_t MergeTreeBitmapIndexReader::DEFAULT_SEGMENT_GRANULARITY = 65536;
const size_t MergeTreeBitmapIndexReader::DEFAULT_SERIALIZING_GRANULARITY = 65536;

MergeTreeBitmapIndexReader::MergeTreeBitmapIndexReader
    (
        const IMergeTreeDataPartPtr & part,
        const BitmapIndexInfoPtr & bitmap_index_info_,
        const MergeTreeIndexGranularity & index_granularity_,
        const MarkRanges & mark_ranges_
    )
    :
    MergeTreeBitmapIndexReader(part, bitmap_index_info_, index_granularity_, DEFAULT_SEGMENT_GRANULARITY, DEFAULT_SERIALIZING_GRANULARITY, mark_ranges_)
{
    
}

MergeTreeBitmapIndexReader::MergeTreeBitmapIndexReader
    (
        const IMergeTreeDataPartPtr & part,
        const BitmapIndexInfoPtr & bitmap_index_info_,
        const MergeTreeIndexGranularity & index_granularity_,
        const size_t & segment_granularity_,
        const size_t & serializing_granularity_, 
        const MarkRanges & mark_ranges_
    )
    :
    bitmap_index_info(bitmap_index_info_),
    index_granularity(index_granularity_),
    segment_granularity(segment_granularity_),
    serializing_granularity(serializing_granularity_),
    mark_ranges(mark_ranges_)
{
    // each column need a reader, load the bitmap index reader for each column
    for (auto & it : bitmap_index_info->index_names)
    {
        if (!valid_reader)
            break;

        try
        {
            for (auto & jt : it.second)
            {
                if (index_name_set.count(jt))
                    continue;

                String index_name = jt;
                index_name_set.insert(index_name);
                
                auto index_reader = std::make_shared<BitmapIndexReader>(part, index_name);
                if (index_reader && index_reader->valid())
                    bitmap_index_readers.insert({index_name, index_reader});
                else
                {
                    valid_reader = false;
                    break;
                }
               
            }
        }
        catch(...)
        {
            tryLogCurrentException(getLogger("MergeTreeBitmapIndexReader"), __PRETTY_FUNCTION__);
            valid_reader = false;
            break;
        }
    }

    // std::cout<<"size of readers: "<<bitmap_index_readers.size()<<std::endl;
    // std::cout<<"size of set_args: "<<bitmap_index_info->set_args.size()<<std::endl;
    // std::cout<<"size of index_names: "<<bitmap_index_info->index_names.size()<<std::endl;
    if (bitmap_index_readers.empty() && segment_bitmap_index_readers.empty())
        valid_reader = false;
}


MergeTreeBitmapIndexReader::~MergeTreeBitmapIndexReader() = default;

/**
 * Default behavior:
 * for a set, it contains multiple elements, each element has a bitmap index
 * this function get index for each element and then OR them by a bit-or operation.
 **/
BitmapIndexPtr MergeTreeBitmapIndexReader::getIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

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
    for (auto & res_indexe : res_indexes)
    {
        bitmap |= res_indexe->getIndex();
    }

    return criterion;
}

/**
 * Default behavior:
 * for a set, it contains multiple elements, each element has a bitmap index
 * this function get index for each element and then OR them by a bit-or operation.
 **/
BitmapIndexPtr MergeTreeBitmapIndexReader::getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::vector<BitmapIndexPtr> res_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    if (to_type.isUInt8())
        getBaseIndex<UInt8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt16())
        getBaseIndex<UInt16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt32())
        getBaseIndex<UInt32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt64())
        getBaseIndex<UInt64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt8())
        getBaseIndex<Int8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt16())
        getBaseIndex<Int16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt32())
        getBaseIndex<Int32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt64())
        getBaseIndex<Int64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isFloat32())
        getBaseIndex<Float32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isFloat64())
        getBaseIndex<Float64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isString())
        getBaseIndex<String>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);

    auto & bitmap = criterion->getIndex();
    for (auto & res_indexe : res_indexes)
    {
        bitmap |= res_indexe->getIndex();
    }

    return criterion;
}

/**
 * Default behavior:
 * for a set, it contains multiple elements, each element has a bitmap index
 * this function get index for each element and then OR them by a bit-or operation.
 **/
BitmapIndexPtr MergeTreeBitmapIndexReader::getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg, const MarkRanges & mark_ranges_inc)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::vector<BitmapIndexPtr> res_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    if (to_type.isUInt8())
        getBaseIndex<UInt8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isUInt16())
        getBaseIndex<UInt16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isUInt32())
        getBaseIndex<UInt32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isUInt64())
        getBaseIndex<UInt64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt8())
        getBaseIndex<Int8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt16())
        getBaseIndex<Int16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt32())
        getBaseIndex<Int32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt64())
        getBaseIndex<Int64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isFloat32())
        getBaseIndex<Float32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isFloat64())
        getBaseIndex<Float64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isString())
        getBaseIndex<String>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);

    auto & bitmap = criterion->getIndex();
    for (auto & res_indexe : res_indexes)
    {
        bitmap |= res_indexe->getIndex();
    }

    return criterion;
}

/**
 ** Default behavior:
 ** index_names: multiple functions -> single column
 ** set_args: multiple functions -> single set
 ** Thus, we construct a map (function name, bitmap index), each bitmap index is consisted of all element in set in a disjunctive way (OR operation).
 **/
void MergeTreeBitmapIndexReader::getIndexes()
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

        if (it->second.empty() || set_it->second.empty())
            throw Exception("Default BitMapIndexReader cannot handle a function with empty column / sets", ErrorCodes::LOGICAL_ERROR);

        auto index_name = it->second.front();
        auto set_ptr = set_it->second.front();

// choose which reader to use
#define SETBITMAPINDEXITER(IT, RD) auto IT = RD.find(index_name);
#define SETBITMAPINDEX(IT, RD) if (IT != RD.end())                              \
        {                                                                       \
            const BitmapIndexPtr & list_index = getIndex(IT->second, set_ptr);  \
            inner_index->setOriginalRows(list_index->getOriginalRows());        \
            inner_index->setIndex(list_index->getIndex());                      \
        }

        SETBITMAPINDEXITER(seg_reader_it, segment_bitmap_index_readers)
        SETBITMAPINDEXITER(reader_it, bitmap_index_readers)
        
        SETBITMAPINDEX(seg_reader_it, segment_bitmap_index_readers)
        else SETBITMAPINDEX(reader_it, bitmap_index_readers)
#undef SETBITMAPINDEXITER
#undef SETBITMAPINDEX
        else
        {
            throw Exception("No BitmapIndexReader of " + index_name + " has been found", ErrorCodes::LOGICAL_ERROR);
        }

        // (column name, index)
        indexes.emplace(it->first, inner_index);
    }
}

/**
 ** Default behavior:
 ** index_names: multiple functions -> single column
 ** set_args: multiple functions -> single set
 ** Thus, we construct a map (function name, bitmap index), each bitmap index is consisted of all element in set in a disjunctive way (OR operation).
 **/
void MergeTreeBitmapIndexReader::getIndexesFromMarkRanges(const MarkRanges & mark_ranges_inc)
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

        if (it->second.empty() || set_it->second.empty())
            throw Exception("Default BitMapIndexReader cannot handle a function with empty column / sets", ErrorCodes::LOGICAL_ERROR);

        auto index_name = it->second.front();
        auto set_ptr = set_it->second.front();

// choose which reader to use
        auto seg_reader_it = segment_bitmap_index_readers.find(index_name);
        auto reader_it = bitmap_index_readers.find(index_name);

        if (seg_reader_it != segment_bitmap_index_readers.end())
        {
            const BitmapIndexPtr & list_index = getIndex(seg_reader_it->second, set_ptr, mark_ranges_inc);
            if (indexes.find(it->first) != indexes.end())
                (indexes)[it->first]->orIndex(list_index->getIndex());
            else
            {
                inner_index->setOriginalRows(list_index->getOriginalRows());
                inner_index->setIndex(list_index->getIndex());
                indexes.emplace(it->first, inner_index);
            }

        }
        else if(reader_it != bitmap_index_readers.end())
        {
            // original bitmap will extract the bitmap of the whole part
            // thus no extra operation needed
            continue;
        }
        else
        {
            throw Exception("No BitmapIndexReader nor SegmentBitmapIndexReader of " + index_name + "has been found", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void MergeTreeBitmapIndexReader::initIndexes(const NameSet & indexes_)
{
    output_indexes.clear();
    for (const auto & name : indexes_)
        output_indexes.insert(name);
    /// Create header
    if (valid_reader)
    {
        /// Create header for this reader
        Block header;
        bitmap_index_info->updateHeader(header, true);
        for (const auto & output : output_indexes)
        {
            if (header.has(output))
            {
                const auto & col = header.getByName(output);
                columns.emplace_back(col.name, col.type);
            }
        }
        getIndexes();
    }
}

// for incrementally added marks
// grabbing corresponding segment bitmap
void MergeTreeBitmapIndexReader::addSegmentIndexesFromMarkRanges(const MarkRanges & mark_ranges_inc)
{
    if (!valid_reader)
        return ;
    for (const auto &mark_range : mark_ranges_inc)
        mark_ranges.push_back(mark_range);
    getIndexesFromMarkRanges(mark_ranges_inc);
}

}
