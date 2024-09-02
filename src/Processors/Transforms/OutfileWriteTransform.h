#pragma once
#include <Processors/IProcessor.h>
#include "DataStreams/IBlockStream_fwd.h"
#include "Formats/FormatFactory.h"
#include "Interpreters/Context_fwd.h"
#include "Processors/Formats/IOutputFormat.h"
#include "Storages/IStorage_fwd.h"

namespace DB
{

class OutfileWriteTransform : public IProcessor
{
public:
    OutfileWriteTransform(OutputFormatPtr output_format_, const Block & header_, const ContextPtr & context_);

    String getName() const override
    {
        return "OutfileWrite";
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
    void consume(Chunk chunk);
    void onFinish();
    Block getHeader();

    InputPort & input;
    OutputPort & output;

    OutputFormatPtr output_format;
    Block header;

    ContextPtr context;

    Chunk current_chunk;
    bool has_input = false;
};

}
