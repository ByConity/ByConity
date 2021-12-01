#include <Processors/Transforms/SubstitutionTransform.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{


SubstitutionTransform::SubstitutionTransform(const Block & header_, const std::unordered_map<String, String> & name_substitution_info_):
    ISimpleTransform(
        header_,
        transformHeader(header_, name_substitution_info_),
        true), name_substitution_info(name_substitution_info_) {}

Block SubstitutionTransform::transformHeader(Block header, const std::unordered_map<String, String> & name_substitution_info_)
{
    substituteBlock(header, name_substitution_info_);
    return header;
}

void SubstitutionTransform::transform(Chunk & chunk)
{
    size_t chunk_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    Block block = getInputPort().getHeader().cloneWithColumns(columns);
    columns.clear();
    substituteBlock(block, name_substitution_info);
    columns = block.getColumns();
    chunk.setColumns(std::move(columns), chunk_rows);
}

}
