#include <Interpreters/BitMapIndexHelper.h>
#include <Storages/MergeTree/MarkRange.h>
// TODO dongyifeng add it later
#include <Storages/MergeTree/MergeTreeBitMapIndexReader.h>
#include <Storages/MergeTree/MergeTreeBitMapIndexBoolReader.h>
//#include <Storages/MergeTree/MergeTreeBitMapIndexSingleReader.h>
#include <Common/Exception.h>
#include <set>
#include <map>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_IDENTIFIER;
extern const int UNEXPECTED_EXPRESSION;
extern const int TYPE_MISMATCH;
extern const int FUNCTION_NOT_ALLOWED;
}

static std::map<String, BitMapIndexReturnType> array_set_functions = {
    {"arraySetCheck", BitMapIndexReturnType::BOOL},
    {"arraySetGet", BitMapIndexReturnType::MULTIPLE},
    {"arraySetGetAny", BitMapIndexReturnType::SINGLE}
};

bool BitMapIndexHelper::isArraySetFunctions(const String & name)
{
    return array_set_functions.count(name);
}

BitMapIndexReturnType BitMapIndexHelper::getBitMapIndexReturnType(const String & name)
{
    auto it = array_set_functions.find(name);
    if (it == array_set_functions.end())
        throw Exception("Cannot find function " + name + " to apply bitmap index", ErrorCodes::FUNCTION_NOT_ALLOWED);
    return it->second;
}

std::unique_ptr<MergeTreeBitMapIndexReader> BitMapIndexHelper::getBitMapIndexReader
    (
        const String & path,
        const BitMapIndexInfoPtr & bitmap_index_info,
        const MergeTreeIndexGranularity & index_granularity,
        const MarkRanges & mark_ranges
    )
{
    if (!bitmap_index_info || bitmap_index_info->return_types.empty())
        return nullptr;

    BitMapIndexReturnType return_type = BitMapIndexReturnType::UNKNOWN;

    for (auto it = bitmap_index_info->return_types.begin(); it != bitmap_index_info->return_types.end(); ++it)
    {
        if (return_type == BitMapIndexReturnType::UNKNOWN)
            return_type = it->second;
        else
        {
            if (return_type != it->second)
                throw Exception("There are multiple return type when get bitmap index reader", ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (return_type == BitMapIndexReturnType::BOOL)
        return std::make_unique<MergeTreeBitMapIndexBoolReader>(path, bitmap_index_info, index_granularity, mark_ranges);
//    else if (return_type == BitMapIndexReturnType::SINGLE)
//        return std::make_unique<MergeTreeBitMapIndexSingleReader>(path, bitmap_index_info, index_granularity, mark_ranges);
    else if (return_type == BitMapIndexReturnType::MULTIPLE)
        return nullptr;
    else
        throw Exception("Cannot get a bitmap index reader since the return type is UNKNOWN", ErrorCodes::LOGICAL_ERROR);
}

}
