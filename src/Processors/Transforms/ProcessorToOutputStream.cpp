#include <math.h>
#include <Processors/Transforms/ProcessorToOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

Block ProcessorToOutputStream::newHeader()
{
    return {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "inserted_rows")};
}

ProcessorToOutputStream::ProcessorToOutputStream(BlockOutputStreamPtr stream_)
    : IProcessor({stream_->getHeader()}, {newHeader()})
    , input(inputs.front())
    , output(outputs.front())
    , stream(std::move(stream_))
{
    total_rows = 0;
    stream->writePrefix();
}

void ProcessorToOutputStream::consume(Chunk chunk)
{
    total_rows += chunk.getNumRows();
    stream->write(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
}

Chunk ProcessorToOutputStream::getReturnChunk()
{
    auto total_rows_column = DataTypeUInt64().createColumnConst(1, total_rows);
    return Chunk({total_rows_column}, 1);
}

void ProcessorToOutputStream::onFinish()
{
    stream->writeSuffix();

    auto return_chunk = getReturnChunk();
    output_data.chunk = std::move(return_chunk);
    output.pushData(std::move(output_data));
}

ProcessorToOutputStream::Status ProcessorToOutputStream::prepare()
{
    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        onFinish();
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void ProcessorToOutputStream::work()
{
    consume(std::move(current_chunk));
    has_input = false;
}

}
