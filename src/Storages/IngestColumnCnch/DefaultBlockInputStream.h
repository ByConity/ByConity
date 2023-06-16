#pragma once
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

class DefaultBlockInputStream : public IBlockInputStream
{
public:
    DefaultBlockInputStream(Block header, size_t rows_count_, size_t min_block_size_rows_);
    String getName() const override { return "Defaulting"; }
    Block getHeader() const override { return header; }

private:
    Block readImpl() override;
    Block makeBlock(size_t block_size);
    Block header;
    size_t rows_count;
    size_t min_block_size_rows;
};

}
