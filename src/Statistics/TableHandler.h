#pragma once
#include <memory>
#include <type_traits>
#include <vector>
#include <Core/Types.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/SubqueryHelper.h>

namespace DB::Statistics
{


// ColumnHandler is a sql generator and result block parser
//    usually used for collecting stats of a column
// getSqls is to generate sql elements, such as min(col), max(col)
// parse is to process the result of the corresponding elements,
//     which are located at index_offset+0, index_offset+1, ... of the block
// ColumnHandlers usually maintain their own context to get the result.
class ColumnHandlerBase
{
public:
    virtual std::vector<String> getSqls() = 0;
    virtual void parse(const Block & block, size_t index_offset) = 0;
    virtual size_t size() = 0;
    virtual ~ColumnHandlerBase() = default;
};

using ColumnHandlerPtr = std::unique_ptr<ColumnHandlerBase>;

// TableHandler generate a full sql, based on registered ColumnHandler
// after executing sql and get block as result, we parse the block
//   with ColumnHander with correct index_offset
class TableHandler
{
public:
    explicit TableHandler(StatsTableIdentifier & table_identifier_) : table_identifier(table_identifier_) { }
    void registerHandler(std::unique_ptr<ColumnHandlerBase> handler);
    String getFullSql();
    std::vector<std::unique_ptr<ColumnHandlerBase>> & getHandlers() { return handlers; }

    void parse(const Block & block);

private:
    StatsTableIdentifier table_identifier;
    std::vector<std::unique_ptr<ColumnHandlerBase>> handlers;
    size_t column_size = 0;
};

}
