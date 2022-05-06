#pragma once

#include <Core/Types.h>
#include <Core/Names.h>
#include <Interpreters/PreparedSets.h>
#include <map>
#include <set>
#include <memory>
#include <deque>

namespace DB
{

/**
 * To determine how to return values if a bitmap index is found,
 * BOOL means only return 1/0 if a row is in bitmap or not
 * SINGLE means return the first value if there is a set of element in one row hit the bitmap
 * MULTIPLE means return all the value that hit the bitmap in one row
 * e.g. 
 * arraySetCheck return bool
 * arraySetGetAny return single
 * arraySetGet return multiple
 **/
enum class BitMapIndexReturnType : UInt8
{
    UNKNOWN,
    BOOL,
    SINGLE,
    MULTIPLE
};

struct BitMapIndexInfo
{
    // key: function result column name
    // value: set ptr
    std::map<String, std::vector<SetPtr>> set_args;
    // key: function result column name
    // value: function target column name
    std::map<String, std::vector<String>> index_names;
    // key: function result column name
    // value: return type of this function
    std::map<String, BitMapIndexReturnType> return_types;

    NameSet index_column_name_set;
    NameSet non_removable_index_columns;

    BitMapIndexInfo() = default;
};

using BitMapIndexInfoPtr = std::shared_ptr<BitMapIndexInfo>;
class MergeTreeBitMapIndexReader;
struct MarkRange;
using MarkRanges = std::deque<MarkRange>;
class MergeTreeIndexGranularity;

class BitMapIndexHelper
{
public:

    static bool isArraySetFunctions(const String & name);

    static BitMapIndexReturnType getBitMapIndexReturnType(const String & name);

    static std::unique_ptr<MergeTreeBitMapIndexReader> getBitMapIndexReader
        (
            const String & path,
            const BitMapIndexInfoPtr & bitmap_index_info,
            const MergeTreeIndexGranularity & index_granularity,
            const MarkRanges & mark_ranges
        );
};

}
