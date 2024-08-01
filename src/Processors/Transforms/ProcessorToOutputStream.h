#pragma once
#include <Processors/IProcessor.h>
#include <DataStreams/IBlockOutputStream.h>

namespace DB
{

class ProcessorToOutputStream : public IProcessor
{
public:
    explicit ProcessorToOutputStream(BlockOutputStreamPtr stream_);

    String getName() const override { return "ProcessorToOutputStream"; }

    static Block newHeader();
    Chunk getReturnChunk();

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

protected:
    InputPort & input;
    OutputPort & output;

    Chunk current_chunk;
    Port::Data output_data;
    bool has_input = false;

    void consume(Chunk chunk);
    void onFinish();

private:
    BlockOutputStreamPtr stream;
    size_t total_rows;

};

}
