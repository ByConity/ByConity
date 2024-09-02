#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/TableWriteTransform.h>
#include <Storages/IStorage.h>
#include <Transaction/ICnchTransaction.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

TableWriteTransform::TableWriteTransform(
    BlockOutputStreamPtr stream_, const Block & header_, const StoragePtr & storage_, const ContextPtr & context_)
    : IProcessor({stream_->getHeader()}, {header_})
    , input(inputs.front())
    , output(outputs.front())
    , stream(stream_)
    , header(header_)
    , storage(storage_)
    , context(context_)
{
}

Block TableWriteTransform::getHeader()
{
    return header;
}

TableWriteTransform::Status TableWriteTransform::prepare()
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

void TableWriteTransform::work()
{
    consume(std::move(current_chunk));
    has_input = false;
}

void TableWriteTransform::consume(Chunk chunk)
{
    stream->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void TableWriteTransform::onFinish()
{
    stream->writeSuffix();
    output.finish();
}

}
