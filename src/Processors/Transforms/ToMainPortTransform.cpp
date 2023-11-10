#include <Processors/Chunk.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/ToMainPortTransform.h>


namespace DB
{

TotalsPortToMainPortTransform::TotalsPortToMainPortTransform(const Block & header_) : ISimpleTransform(header_, header_, false)
{
}

void TotalsPortToMainPortTransform::transform(Chunk & chunk)
{
    if ((chunk = IOutputFormat::prepareTotals(std::move(chunk))))
    {
        auto chunk_info = std::make_shared<ChunkInfoTotals>();
        chunk.setChunkInfo(std::move(chunk_info));
    }
}

ExtremesPortToMainPortTransform::ExtremesPortToMainPortTransform(const Block & header_) : ISimpleTransform(header_, header_, false)
{
}

void ExtremesPortToMainPortTransform::transform(Chunk & chunk)
{
    if (chunk)
    {
        auto chunk_info = std::make_shared<ChunkInfoExtremes>();
        chunk.setChunkInfo(std::move(chunk_info));
    }
}

}
