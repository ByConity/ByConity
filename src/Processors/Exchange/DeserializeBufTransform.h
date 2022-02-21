#pragma once
#include <Processors/Chunk.h>
#include <butil/iobuf.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
class DeserializeBufTransform : public ISimpleTransform
{
public:
    
    struct IOBufChunkInfo : public ChunkInfo
    {
        butil::IOBuf io_buf;
    };

    explicit DeserializeBufTransform(const Block & header_, bool enable_block_compress_);

    String getName() const override { return "DeserializeBufTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const Block & header;
    bool enable_block_compress;
    Poco::Logger * logger;

};

}
