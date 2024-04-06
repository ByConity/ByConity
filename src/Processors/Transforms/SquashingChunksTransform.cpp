#include <Processors/Transforms/SquashingChunksTransform.h>
#include <iostream>

namespace DB
{

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_)
    : ISimpleTransform(header, header, true), squashing(min_block_size_rows, min_block_size_bytes, reserve_)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
            chunk.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        auto block = squashing.add({});
        chunk.setColumns(block.getColumns(), block.rows());
    }
}

IProcessor::Status SimpleSquashingChunksTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        finished = true;
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}

}
