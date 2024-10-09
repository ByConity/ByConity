#pragma once
#include <DataStreams/IBlockOutputStream.h>
#include <Processors/IProcessor.h>
#include "IO/Progress.h"

namespace DB
{

class ProcessorToOutputStream : public IProcessor
{
public:
    explicit ProcessorToOutputStream(BlockOutputStreamPtr stream_, const String & output_inserted_rows_name_);

    String getName() const override { return "ProcessorToOutputStream"; }

    static Block newHeader(const String & output_inserted_rows_name);
    Chunk getReturnChunk();

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }
    void setProgressCallback(ProgressCallback progress_callback_)
    {
        progress_callback = std::move(progress_callback_);
    }

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
    ProgressCallback progress_callback;
};

}
