#include <math.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/ProcessorToOutputStream.h>
#include "common/types.h"
#include "IO/Progress.h"

namespace DB
{

Block ProcessorToOutputStream::newHeader(const String & output_inserted_rows_name)
{
    return {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), output_inserted_rows_name)};
}

ProcessorToOutputStream::ProcessorToOutputStream(BlockOutputStreamPtr stream_, const String & output_inserted_rows_name_)
    : IProcessor({stream_->getHeader()}, {newHeader(output_inserted_rows_name_)})
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
    if (progress_callback)
    {
        WriteProgress write_progress(total_rows, 0);
        progress_callback(Progress(write_progress));
    }
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
