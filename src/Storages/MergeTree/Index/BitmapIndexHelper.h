#pragma once

#include <Core/Types.h>
#include <Core/Names.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/Index/MergeTreeIndexHelper.h>
#include <Core/Block.h>
#include <Parsers/ASTFunction.h>
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
enum class BitmapIndexReturnType : UInt8
{
    UNKNOWN,
    BOOL,
    SINGLE,
    MULTIPLE,
    EXPRESSION,
    INTERSECT_BOOL, // For hasAll, do the intersection for each bitmap
};

class Block;

const char * typeToString(BitmapIndexReturnType t);

class BitmapIndexInfo : public MergeTreeIndexInfo
{
public: 
    // key: function result column name
    // value: set ptr
    std::map<String, std::vector<SetPtr>> set_args;
    // key: function result column namep
    // value: function target column name
    std::map<String, std::vector<String>> index_names;
    // key: function result column name
    // value: return type of this function
    std::map<String, BitmapIndexReturnType> return_types;
    // key: function result column name
    // value: bitmap expression to eval
    std::map<String, String> bitmap_expressions;

    NameSet index_column_name_set;
    NameSet non_removable_index_columns;
    NameSet remove_on_header_column_name_set;

    BitmapIndexInfo() : MergeTreeIndexInfo(MergeTreeIndexInfo::Type::BITMAP) {}

    void buildIndexInfo(const ASTPtr & node, BuildIndexContext & building_context, const StorageMetadataPtr & metadata_snapshot) override;
    void setNonRemovableColumn(const String & column) override;

    std::pair<NameSet,NameSet> getIndexColumns(const IMergeTreeDataPartPtr & data_part) override;

    void initIndexes(const Names & columns) override;
    
    String toString() const override;

    String dump() const;

    void updateHeader(Block & header, bool remove_indexed_columns = true) const;

    Block updateHeader(Block header);

};

using BitmapIndexInfoPtr = std::shared_ptr<BitmapIndexInfo>;
class MergeTreeBitmapIndexReader;
struct MarkRange;
using MarkRanges = std::deque<MarkRange>;
class MergeTreeIndexGranularity;

class BitmapIndexHelper
{
public:

    // check set arguments do not have null value
    static bool hasNullArgument(const ASTPtr & ast);
    // Just support literal & const function
    static bool checkConstArguments(const ASTPtr & ast, bool skip_even_columns = true);

    static bool isArraySetFunctions(const String & name);
    static bool isNarrowArraySetFunctions(const String & name);

    static bool isBitmapFunctions(const String & name);

    static bool isValidBitMapFunctions(const ASTPtr & ast);

    static BitmapIndexReturnType getBitmapIndexReturnType(const String & name);

    static std::unique_ptr<MergeTreeBitmapIndexReader> getBitmapIndexReader
        (
            const IMergeTreeDataPartPtr & part,
            const BitmapIndexInfoPtr & bitmap_index_info,
            const MergeTreeIndexGranularity & index_granularity,
            const size_t & segment_granularity, 
            const size_t & serializing_granularity, 
            const MarkRanges & mark_ranges
        );
        
    static std::unique_ptr<MergeTreeBitmapIndexReader> getBitmapIndexReader
        (
            const IMergeTreeDataPartPtr & part,
            const BitmapIndexInfoPtr & bitmap_index_info,
            const MergeTreeIndexGranularity & index_granularity,
            const MarkRanges & mark_ranges
        );
};

bool functionCanUseBitmapIndex(const ASTFunction & function);
}
