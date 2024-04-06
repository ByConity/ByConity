#pragma once

#include <DataStreams/SquashingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IProcessor.h>
#include <Core/Block.h>
namespace DB
{
/// Doesn't care about propagating exceptions and thus doesn't throw LOGICAL_ERROR if the following transform closes its input port.
class SimpleSquashingChunksTransform : public ISimpleTransform
{
public:
    explicit SimpleSquashingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_ = false);

    String getName() const override { return "SimpleSquashingTransform"; }

protected:
    void transform(Chunk &) override;

    IProcessor::Status prepare() override;

private:
    SquashingTransform squashing;

    /// When consumption is finished we need to release the final chunk regardless of its size.
    bool finished = false;
};
}
