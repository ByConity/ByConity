#include <Processors/Transforms/ReverseTransform.h>
#include <Common/PODArray.h>

namespace DB
{

void ReverseTransform::transform(Chunk & chunk)
{
    IColumn::Permutation permutation;

    size_t num_rows = chunk.getNumRows();
    for (size_t i = 0; i < num_rows; ++i)
        permutation.emplace_back(num_rows - 1 - i);

    auto columns = chunk.detachColumns();

    for (auto & column : columns)
        column = column->permute(permutation, 0);

    if (auto * side_block = chunk.getSideBlock())
    {
        for (size_t i = 0; i < side_block->columns(); ++i)
        {
            auto & side_column = side_block->getByPosition(i).column;
            side_column = side_column->permute(permutation, 0);
        }
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
