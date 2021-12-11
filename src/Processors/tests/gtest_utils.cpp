#include <Processors/tests/gtest_utils.h>
namespace UnitTest {

DB::Chunk createUInt8Chunk(size_t row_num, size_t column_num, UInt8 value)
{
    DB::Columns columns;
    for (size_t i = 0; i < column_num; i++)
    {
        auto col = DB::ColumnUInt8::create(row_num, value);
        columns.emplace_back(std::move(col));
    }
    return DB::Chunk(std::move(columns), row_num);
}

}
