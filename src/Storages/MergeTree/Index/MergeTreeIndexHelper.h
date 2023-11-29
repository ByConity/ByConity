#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <deque>
#include <Core/Types.h>
#include <Core/Block.h>
#include <Storages/MergeTree/Index/MergeTreeColumnIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ExpressionActions.h>
#include <QueryPlan/Assignment.h>


namespace DB
{

class MergeTreeIndexInfo;
using MergeTreeIndexInfoPtr = std::shared_ptr<MergeTreeIndexInfo>;

class IMergeTreeDataPart;
using IMergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

class MergeTreeIndexInfo
{
public:
    enum class Type : UInt8
    {
        UNKNOWN = 0,
        BITMAP
    };

    struct BuildIndexContext
    {
        const NamesAndTypesList & input_columns;
        const NamesAndTypesList & required_columns;
        const ContextPtr & context;
        PreparedSets & prepared_sets;
        const NamesWithAliases & outputs;
    };

    explicit MergeTreeIndexInfo(Type type_)
    : type(type_) {}

    virtual ~MergeTreeIndexInfo() = default;

    virtual Type getType() const { return type; }

    virtual void buildIndexInfo(const ASTPtr & node, BuildIndexContext & building_context) = 0;

    virtual void setNonRemovableColumn(const String & column) = 0;

    virtual std::pair<NameSet,NameSet> getIndexColumns(const IMergeTreeDataPartPtr & data_part) = 0;

    virtual void initIndexes(const Names & columns) = 0;

    virtual String toString() const = 0;

private:

    Type type;
};

inline String IndexTypeToString(MergeTreeIndexInfo::Type type)
{
    switch(type)
    {
        case MergeTreeIndexInfo::Type::UNKNOWN:
            return "UNKNOWN";
        case MergeTreeIndexInfo::Type::BITMAP:
            return "BITMAP";
    }
    return "";
}

struct MarkRange;
using MarkRanges = std::deque<MarkRange>;
class Context;

class MergeTreeIndexExecutor;
using MergeTreeIndexExecutorPtr = std::shared_ptr<MergeTreeIndexExecutor>;

class MergeTreeIndexContext;
using MergeTreeIndexContextPtr = std::shared_ptr<MergeTreeIndexContext>;
class ProjectionStep;

struct BitmapIndexChecker
{
    mutable std::mutex mutex;
    std::unordered_map<String, bool> bitmap_indexes;

    bool hasBitmapIndex(const String & name) const {
        auto lock = std::lock_guard<std::mutex>(mutex);
        return bitmap_indexes.find(name) != bitmap_indexes.end();
    }

    void emplace(const String & name, bool has_index) {
        auto lock = std::lock_guard<std::mutex>(mutex);
        bitmap_indexes.emplace(name, has_index);
    }

    static void checkBitmapIndex(const IMergeTreeDataPartPtr & part);
    static void checkSegmentBitmapIndex(const IMergeTreeDataPartPtr & part);
};

using BitmapIndexCheckerPtr = std::shared_ptr<BitmapIndexChecker>;

class MergeTreeIndexContext
{

public:

    // TODO: make it unique ptr
    MergeTreeIndexExecutorPtr getIndexExecutor(
        const IMergeTreeDataPartPtr & part,
        const MergeTreeIndexGranularity & index_granularity,
        size_t index_segment_granularity,
        size_t index_serializing_granularity,
        const MarkRanges & mark_ranges);

    void add(const MergeTreeIndexInfoPtr & info);

    MergeTreeIndexInfoPtr get(MergeTreeIndexInfo::Type type);
    bool has(MergeTreeIndexInfo::Type type);

    String toString() const;

    void setProjection(const ActionsDAGPtr & project_actions_) { project_actions = project_actions_; }

    ActionsDAGPtr getProjectionActions(MergeTreeIndexExecutor * executor) const;
    ActionsDAGPtr getProjectionActions(bool has_index) const;
    ActionsDAGPtr getProjectionActionsForColumn() const { return project_actions; }

    /**
    * for each projection, it contains {from_name, to_name}, 
    * from_name is column read (calculated) from storage
    * to_name is column we want to return (after projection).
    *
    * if the index is not be calculated, we need to execute the from_name, the result is to_name.
    * if the index is be executed to from_name, we just to project from_name to to_name.
    *
    * they are very different, e.g.
    * 
    * select count() from db.table where arraySetCheck(vid, 1);
    * the projection may be {arraySetCheck(vid, 1), arraySetCheck(vid, 1)_1};
    * if vid index is not calculated, we get vid from storage, then we execute the projection actions
    * that will execute arraySetCheck(vid, 1), then project it to arraySetCheck(vid, 1)_1;
    *
    * if vid index is calculated, we get column arraySetCheck(vid, 1), then we just need to project it to final column. 
    * 
    * executeProjectionFromColumn is the first case.
    * executeProjectionFromMaterializedIndex is the second case.
    */
    void executeProjectionForColumn(Block & block);
    void executeProjectionForMaterializedIndex(Block & block);

    void makeProjectionForMaterializedIndex( const ProjectionStep * projection, MergeTreeIndexInfo::BuildIndexContext & building_context);

    static MergeTreeIndexContextPtr buildFromProjection(const Assignments & inline_expressions, MergeTreeIndexInfo::BuildIndexContext & building_context);

    bool enable_read_bitmap_index = true;

    /**
     * Get projection from pushdown_projection,
     * the assignment result is (to_column, from_column),
     * where 
     *   - from_column is original column from storage or the function based on the column.
     *   - to_column is the column we provide to subsequent processor.
     * for example, pushdown_projection gives the assignment (hash_uid_1, hash_uid_1), (func_1, func);
     * we get a new assignment (hash_uid, hash_uid, func_1, func),
     * the projection of hash_uid -> hash_uid_1 will be done by ReadFromSourceStep itsel.
     */
    static std::shared_ptr<ProjectionStep> genProjectionForIndex(
        const NamesWithAliases & column_alias,
        const ProjectionStep * pushdown_projection
    );

    MergeTreeIndexContextPtr clone();

private:

    std::map<MergeTreeIndexInfo::Type, MergeTreeIndexInfoPtr> index_infos;

    ActionsDAGPtr project_actions;
    ActionsDAGPtr project_actions_from_materialized_index;

    mutable std::shared_mutex index_lock;

};

class MergeTreeIndexExecutor
{

public:
    enum class ExecuteType : UInt8
    {
        FILTER,
        MATERIALIZE,
    };

    bool hasReaders() const { return !index_readers.empty(); }

    bool valid() const;

    size_t read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res);

    void addReader(MergeTreeIndexInfo::Type type, const MergeTreeColumnIndexReaderPtr & index_reader);
    
    MergeTreeColumnIndexReaderPtr getReader(MergeTreeIndexInfo::Type type) const;

    void initReader(MergeTreeIndexInfo::Type type, const NameSet & columns);

private:

    // ExecutingTree executing_tree;
    // ExecuteType type;

    std::map<MergeTreeIndexInfo::Type, MergeTreeColumnIndexReaderPtr> index_readers;

};

}
