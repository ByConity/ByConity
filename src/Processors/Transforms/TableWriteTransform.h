#pragma once
#include <Processors/IProcessor.h>
#include "DataStreams/IBlockStream_fwd.h"
#include "Interpreters/Context_fwd.h"
#include "Storages/IStorage_fwd.h"

namespace DB
{

class TableWriteTransform : public IProcessor
{
public:
    TableWriteTransform(BlockOutputStreamPtr stream_, const Block & header_, const StoragePtr & storage_, const ContextPtr & context_);

    String getName() const override
    {
        return "TableWrite";
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

    BlockOutputStreamPtr stream;
    Block header;

    StoragePtr storage;
    ContextPtr context;

    Chunk current_chunk;
    bool has_input = false;
};

}
