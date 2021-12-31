#include <Processors/QueryCache.h>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void QueryResult::addResult(const Chunk & chunk)
{
    if (!result)
    {
        result = Chunk(chunk.getColumns(), chunk.getNumRows());
    }
    else
    {
        size_t num_rows = result.getNumRows() + chunk.getNumRows();

        MutableColumns res_columns = result.mutateColumns();
        size_t column_size = res_columns.size();

        Columns add_columns = chunk.getColumns();

        for (size_t i = 0; i < column_size; ++i)
        {
            size_t length = add_columns[i]->size();
            res_columns[i]->insertRangeFrom(*add_columns[i], 0, length);
        }

        result.setColumns(std::move(res_columns), num_rows);
    }
}

}
