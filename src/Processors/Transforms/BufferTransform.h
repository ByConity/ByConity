#pragma once
#include <Common/Logger.h>
#include <list>
#include <Processors/IProcessor.h>

namespace DB
{

class BufferTransform : public IProcessor
{
public:
    BufferTransform(const Block & header, size_t max_queue_size_ = std::numeric_limits<size_t>::max())
        : IProcessor(InputPorts(1, header), OutputPorts(1, header)), max_queue_size(max_queue_size_)
    {
    }

    String getName() const override
    {
        return "Buffer";
    }
    Status prepare() override
    {
        auto & input = inputs.front();
        auto & output = outputs.front();

        if (input.hasData())
        {
            if (output_queue.size() >= max_queue_size)
                return Status::PortFull;
            output_queue.push_back(input.pull());
            max_used_queue_size = std::max(max_used_queue_size, output_queue.size());
            ++input_chunk_count;
        }

        if (output.canPush())
        {
            if (output_queue.empty() && input.isFinished())
            {
                output.finish();
            }
            else if (!output_queue.empty())
            {
                output.push(std::move(output_queue.front()));
                output_queue.pop_front();
            }
        }

        input.setNeeded();

        if (input.isFinished() && output.isFinished())
        {
            LOG_DEBUG(
                getLogger("BufferTransform"),
                "max_used_queue_size:{}/{}, input:[rows:{} bytes:{}], output:[rows:{} bytes:{}]",
                max_used_queue_size,
                input_chunk_count,
                input.getRows(),
                input.getBytes(),
                output.getRows(),
                output.getBytes());
            return Status::Finished;
        }
        else if (input.isFinished())
        {
            return Status::PortFull;
        }
        else
        {
            return Status::NeedData;
        }
    }

private:
    size_t max_queue_size;
    size_t max_used_queue_size = 0;
    size_t input_chunk_count = 0;
    std::list<Chunk> output_queue;
};

}
