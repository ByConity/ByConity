#include <Storages/MergeTree/Index/MergeTreeBitmapIndexExpressionReader.h>
#include <AggregateFunctions/AggregateBitmapExpressionCommon.h>

namespace DB
{
MergeTreeBitmapIndexExpressionReader::MergeTreeBitmapIndexExpressionReader(
    const IMergeTreeDataPartPtr & part_, const BitmapIndexInfoPtr & bitmap_index_info_, const MergeTreeIndexGranularity & index_granularity_, const MarkRanges & mark_ranges_)
    : MergeTreeBitmapIndexReader(part_, bitmap_index_info_, index_granularity_, mark_ranges_)
{
}

MergeTreeBitmapIndexExpressionReader::MergeTreeBitmapIndexExpressionReader(
    const IMergeTreeDataPartPtr & part_, const BitmapIndexInfoPtr & bitmap_index_info_, const MergeTreeIndexGranularity & index_granularity_, 
    const size_t & segment_granularity_, const size_t & serializing_granularity_, 
    const MarkRanges & mark_ranges_)
    : MergeTreeBitmapIndexReader(part_, bitmap_index_info_, index_granularity_, segment_granularity_, serializing_granularity_, mark_ranges_)
{
}

/// TODO: fix it when support `bitmap index remove where condition`
void MergeTreeBitmapIndexExpressionReader::getIndexes()
{
//     // already loaded，try get cache
//     bool expected = false;
//     bool desired = true;

//     auto temp_index_map = std::make_shared<std::map<String, BitmapIndexPtr>>();
//     // for each bitmap, load the needed posting list
//     for (auto it = bitmap_index_info->index_names.begin(); it != bitmap_index_info->index_names.end(); ++it)
//     {
//         BitmapIndexPtr inner_index = std::make_shared<IListIndex>();

//         auto set_it = bitmap_index_info->set_args.find(it->first);
//         if (set_it == bitmap_index_info->set_args.end())
//             throw Exception("Cannot find set for column " + it->first, ErrorCodes::LOGICAL_ERROR);

//         if (it->second.size() > 1 || set_it->second.size() > 1)
//             throw Exception("Default BitmapIndexReader cannot handle a function with multiple column / sets", ErrorCodes::LOGICAL_ERROR);

//         if (it->second.empty() || set_it->second.empty())
//             throw Exception("Default BitmapIndexReader cannot handle a function with empty column / sets", ErrorCodes::LOGICAL_ERROR);

//         auto index_name = it->second.front();
//         auto set_ptr = set_it->second.front();

//         // auto reader_it = bitmap_index_readers.find(index_name);
//         // if (reader_it == bitmap_index_readers.end())
//         //     throw Exception("No BitmapIndexReader of " + index_name + "has been found", ErrorCodes::LOGICAL_ERROR);

//         // const BitmapIndexPtr & list_index = getIndex(reader_it->second, set_ptr);
//         // inner_index->setOriginalRows(list_index->getOriginalRows());
//         // inner_index->setIndex(list_index->getIndex());

//         // supporting segment bitmap
// #define SETBITMAPINDEXITER(IT, RD) auto (IT) = (RD).find(index_name);
// #define SETBITMAPINDEX(IT, RD) if ((IT) != (RD).end())                                  \
//             {                                                                       \
//                 /* load and merge the postings of a set */                                                                  \
//                 const BitmapIndexPtr & list_index = getIndex((IT)->second, set_ptr);  \
//                     inner_index->setOriginalRows(list_index->getOriginalRows());    \
//                     inner_index->setIndex(list_index->getIndex());                  \
//             }

//             SETBITMAPINDEXITER(seg_reader_it, segment_bitmap_index_readers)
//             SETBITMAPINDEXITER(reader_it, bitmap_index_readers)
            
//             SETBITMAPINDEX(seg_reader_it, segment_bitmap_index_readers)
//             else SETBITMAPINDEX(reader_it, bitmap_index_readers)
// #undef SETBITMAPINDEXITER
// #undef SETBITMAPINDEX
//             else
//             {
//                 throw Exception("No BitmapIndexReader nor SegmentBitmapIndexReader of " + index_name + "has been found", ErrorCodes::LOGICAL_ERROR);
//             }
//         // (column name, index)
//         temp_index_map->emplace(it->first, inner_index);
//     }

//     /// cacl bitmap expression
//     for (const auto & bitmap_expression : bitmap_index_info->bitmap_expressions)
//     {
//         AggregateFunctionBitmapDataImpl<String, BitMap> data;
//         BitmapExpressionAnalyzerImpl<String, BitMap> expression_analyzer(bitmap_expression.second);
//         for (const auto & ele : *temp_index_map)
//             data.add(genBitmapKey(ele.first), ele.second->getIndex());
//         expression_analyzer.executeExpression(data);
//         auto inner_index = std::make_shared<IListIndex>();
//         inner_index->setOriginalRows(temp_index_map->begin()->second->getOriginalRows());
//         inner_index->getIndex().loadBitmap(std::move(data.bitmap_map[expression_analyzer.final_key]));
//         indexes.emplace(bitmap_expression.first, inner_index);
//     }

}

size_t MergeTreeBitmapIndexExpressionReader::read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res)
{
    auto result_type = std::make_shared<DataTypeUInt8>();

    size_t prev_rows_start = 0;
    size_t prev_res_num = 0;
    size_t offset = 0;
    for (const auto & output_name : output_indexes)
    {
        prev_rows_start = rows_start;
        if (!res[offset])
            res[offset] = result_type->createColumn();
        MutableColumnPtr column = res[offset++]->assumeMutable();

        auto & res_vec = typeid_cast<ColumnUInt8 &>(*column).getData();
        size_t initial_size = res_vec.size();

        if (!continue_reading)
        {
            /// TODO: check
            if (from_mark == 0)
                prev_rows_start = 0;
            else
                prev_rows_start = index_granularity.getMarkRows(from_mark-1);
        }

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

String MergeTreeBitmapIndexExpressionReader::genBitmapKey(const String & string)
{
    /// '&' ; '|' ; '~' ; ',' ; '#' ;  ' ' ; '(' ; ')' ;  '_';
    static std::unordered_map<char, char> replace_map = {
        {'&', '1'},
        {'|', '2'},
        {'~', '3'},
        {',', '4'},
        {'#', '5'},
        {'(', '6'},
        {')', '7'},
        {'_', '8'},
        {' ', '9'},
        {'\'', 'a'},
    };
    String res;
    res.resize(string.size());
    const char * data = string.data();
    for (size_t i = 0; i < string.size(); ++i)
    {
        auto it = replace_map.find(data[i]);
        res[i] = it == replace_map.end() ? data[i] : it->second;
    }
    return res;
}
}
