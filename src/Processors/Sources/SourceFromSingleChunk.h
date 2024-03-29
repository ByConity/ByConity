#pragma once
#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{

class SourceFromSingleChunk : public SourceWithProgress
{
public:
    explicit SourceFromSingleChunk(Block header, Chunk chunk_) : SourceWithProgress(std::move(header)), chunk(std::move(chunk_)) {}
    explicit SourceFromSingleChunk(Block data) : SourceWithProgress(data.cloneEmpty()), chunk(data.getColumns(), data.rows()) {}
    String getName() const override { return "SourceFromSingleChunk"; }

protected:
    Chunk generate() override { return std::move(chunk); }

private:
    Chunk chunk;
};

}
