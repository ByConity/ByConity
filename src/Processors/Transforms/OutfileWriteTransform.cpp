#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/OutfileWriteTransform.h>
#include <Storages/IStorage.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include "Formats/FormatFactory.h"
#include "Processors/Formats/IOutputFormat.h"
#include "Transaction/ICnchTransaction.h"

namespace DB
{

OutfileWriteTransform::OutfileWriteTransform(
    OutputFormatPtr output_format_, const Block & header_, const ContextPtr & context_)
    : IProcessor({header_}, {header_})
    , input(inputs.front())
    , output(outputs.front())
    , output_format(output_format_)
    , header(header_)
    , context(context_)
{
}

Block OutfileWriteTransform::getHeader()
{
    return header;
}

OutfileWriteTransform::Status OutfileWriteTransform::prepare()
{
    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        onFinish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void OutfileWriteTransform::work()
{
    consume(std::move(current_chunk));
    has_input = false;
}

void OutfileWriteTransform::consume(Chunk chunk)
{
    output_format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void OutfileWriteTransform::onFinish()
{
    output_format->flush();
    output_format->closeFile();
    output.finish();
}

}
