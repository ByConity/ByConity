#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/TableFinishTransform.h>
#include <Storages/IStorage.h>
#include <Transaction/ICnchTransaction.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

TableFinishTransform::TableFinishTransform(const Block & header_, const StoragePtr & storage_, const ContextPtr & context_)
    : IProcessor({header_}, {header_}), input(inputs.front()), output(outputs.front()), storage(storage_), context(context_)
{
}

Block TableFinishTransform::getHeader()
{
    return header;
}

TableFinishTransform::Status TableFinishTransform::prepare()
{
    if (output.isFinished())
    {
        output.finish();
        if (has_output && output_chunk)
        {
            output.push(std::move(output_chunk));
            has_output = false;
        }

        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;

        return Status::PortFull;
    }

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

void TableFinishTransform::work()
{
    consume(std::move(current_chunk));
    has_input = false;
}

void TableFinishTransform::consume(Chunk chunk)
{
    output_chunk = std::move(chunk);
    has_output = true;
}

void TableFinishTransform::onFinish()
{
    TransactionCnchPtr txn = context->getCurrentTransaction();
    txn->setMainTableUUID(storage->getStorageUUID());
    txn->commitV2();
    LOG_DEBUG(&Poco::Logger::get("TableFinishTransform"), "Finish insert select commit in table finish.");
    output.finish();
}

}
