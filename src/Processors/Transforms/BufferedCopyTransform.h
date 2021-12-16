#pragma once
#include <list>
#include <vector>
#include <Processors/IProcessor.h>

namespace DB
{
/// Transform which has single input and num_outputs outputs.
/// Read chunk from input and copy it to all output queues.
class BufferedCopyTransform : public IProcessor
{
public:
    BufferedCopyTransform(const Block & header, size_t num_outputs, size_t max_queue_size_);

    String getName() const override { return "BufferedCopy"; }
    Status prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports) override;
    InputPort & getInputPort() { return inputs.front(); }

private:
    Chunk chunk;
    bool has_data = false;
    bool pushed = false;
    size_t max_queue_size;
    std::vector<char> was_output_processed;
    std::vector<std::list<Chunk>> output_queues;
    std::vector<OutputPort *> output_vec;
    Status prepareGenerate();
    Status prepareConsume(const PortNumbers & updated_output_ports);
    void tryFlush(const PortNumbers & updated_output_ports);
};

}
