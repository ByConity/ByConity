#include <Storages/IngestColumnCnch/DefaultBlockInputStream.h>


namespace DB
{
DefaultBlockInputStream::DefaultBlockInputStream(Block header_, size_t rows_count_, size_t min_block_size_rows_)
    : header{std::move(header_)},
      rows_count{rows_count_},
      min_block_size_rows{min_block_size_rows_}
{
}

Block DefaultBlockInputStream::readImpl()
{
    if (!rows_count)
        return {};

    if (rows_count < min_block_size_rows)
    {
        size_t block_size = rows_count;
        rows_count = 0;
        return makeBlock(block_size);
    }
    else
    {
        rows_count -= min_block_size_rows;
        return makeBlock(min_block_size_rows);
    }
}

Block DefaultBlockInputStream::makeBlock(size_t block_size)
{
    Block res = header.cloneEmpty();

    const ColumnsWithTypeAndName & columns_with_type_name= res.getColumnsWithTypeAndName();
    MutableColumns block_columns(columns_with_type_name.size());
    for (size_t i = 0; i < block_columns.size(); ++i)
    {
        block_columns[i] = columns_with_type_name[i].type->createColumn()->cloneResized(block_size);
    }

    res.setColumns(std::move(block_columns));
    return res;
}

}
