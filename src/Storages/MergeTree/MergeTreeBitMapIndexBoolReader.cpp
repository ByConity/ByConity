#include <Storages/MergeTree/MergeTreeBitMapIndexBoolReader.h>
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

MergeTreeBitMapIndexBoolReader::MergeTreeBitMapIndexBoolReader
    (
        const String & path_,
        const BitMapIndexInfoPtr & bitmap_index_info_,
        const MergeTreeIndexGranularity & index_granularity_,
        const MarkRanges & mark_ranges_
    )
    : MergeTreeBitMapIndexReader(path_, bitmap_index_info_, index_granularity_, mark_ranges_)
{
}

/** main logic of bitmap index read */
size_t MergeTreeBitMapIndexBoolReader::read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res)
{
    auto result_type = std::make_shared<DataTypeUInt8>();

    size_t prev_rows_start = 0;
    size_t prev_res_num = 0;
    for (const auto & output_name : output_indexes)
    {
        prev_rows_start = rows_start;

//        bool append = res.has(output_name);
//        if (!append)
//            res.insert(ColumnWithTypeAndName(result_type->createColumn(), result_type, output_name));

        res.emplace_back(result_type->createColumn());
//        MutableColumnPtr column = res.getByName(output_name).column->assumeMutable();
        MutableColumnPtr column = res[res.size() - 1]->assumeMutable();

        auto & res_vec = typeid_cast<ColumnUInt8 &>(*column).getData();
        size_t initial_size = res_vec.size();

        if (!continue_reading)
//            prev_rows_start = from_mark * index_granularity;
            prev_rows_start = index_granularity.getMarkStartingRow(from_mark);

        auto index_it = indexes.find(output_name);
        if (index_it == indexes.end())
            continue;

        size_t rows_end = std::min(prev_rows_start + max_rows_to_read, index_it->second->getOriginalRows());

        if (rows_end > prev_rows_start)
            res_vec.resize_fill(initial_size + rows_end - prev_rows_start);

        const auto & bit_map = index_it->second->getIndex();
        Roaring::const_iterator begin = bit_map.begin();
        begin.equalorlarger(prev_rows_start);

        // std::cout<<"rows_start: "<<prev_rows_start<<" rows_end: "<<rows_end
        //     <<" index rows: "<<index_it->second->getOriginalRows()<<" begin is end()? :"<<(begin == bit_map.end() || *begin >= rows_end)<<std::endl;

        for (; begin != bit_map.end(); begin++)
        {
            if (*begin >= rows_end)
                break;
            res_vec[initial_size + *begin - prev_rows_start] = 1;
        }

        size_t res_num = rows_end - prev_rows_start;
        prev_rows_start = rows_end;
        if (prev_res_num == 0)
            prev_res_num = res_num;
        else if (prev_res_num != res_num)
            throw Exception("Mismatch rows number of " + output_name, ErrorCodes::LOGICAL_ERROR);
    }

    rows_start = prev_rows_start;
    return prev_res_num;
}

// get index of conjunct functions
void MergeTreeBitMapIndexBoolReader::getIndexes()
{
    // for each bitmap, load the needed posting list
    for (auto it = bitmap_index_info->index_names.begin(); it != bitmap_index_info->index_names.end(); ++it)
    {
        BitmapIndexPtr inner_index = std::make_shared<IListIndex>();
        auto & inner_bitmap = inner_index->getIndex();

        for (size_t idx = 0; idx < it->second.size(); ++idx)
        {
            String index_name = it->second.at(idx);

            auto reader_it = bitmap_index_readers.find(index_name);
            if (reader_it == bitmap_index_readers.end())
                throw Exception("No BitmapIndexReader of " + index_name + "has been found", ErrorCodes::LOGICAL_ERROR);
            auto set_it = bitmap_index_info->set_args.find(it->first);
            if (set_it == bitmap_index_info->set_args.end())
                throw Exception("Cannot find set for column " + it->first, ErrorCodes::LOGICAL_ERROR);

            auto set_ptr = set_it->second.at(idx);
            // load and merge the postings of a set
            const BitmapIndexPtr & list_index = getIndex(reader_it->second, set_ptr);

            if (inner_index->getOriginalRows() == 0)
            {
                inner_index->setOriginalRows(list_index->getOriginalRows());
                inner_index->setIndex(list_index->getIndex());
                continue;
            }
            // bitand for variadic arraySetCheck function
            inner_bitmap &= list_index->getIndex();
        }

        // (column name, index)
        indexes.emplace(it->first, inner_index);
    }
}
}
