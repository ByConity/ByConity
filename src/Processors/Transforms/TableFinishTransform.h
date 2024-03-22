#pragma once
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/IProcessor.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/CnchLock.h>

namespace DB
{

class TableFinishTransform : public IProcessor
{
public:
    TableFinishTransform(const Block & header_, const StoragePtr & storage_, const ContextPtr & context_, ASTPtr & query_);

    String getName() const override
    {
        return "TableFinish";
    }

    Status prepare() override;
    void work() override;

    InputPort & getInputPort()
    {
        return input;
    }
    OutputPort & getOutputPort()
    {
        return output;
    }

private:
    void consume(Chunk block);
    void onFinish();
    Block getHeader();

    InputPort & input;
    OutputPort & output;

    Block header;

    Chunk current_chunk;
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;

    StoragePtr storage;
    ContextPtr context;
    ASTPtr query;
    CnchLockHolderPtrs lock_holders;
};

}
