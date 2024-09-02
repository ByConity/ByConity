#include <cstddef>
#include <memory>
#include <math.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/Transforms/ExpandTransform.h>
namespace DB
{

ExpandTransform::ExpandTransform(const Block & header_, const Block & output_header_, std::vector<ExpressionActionsPtr> expressions_)
    : ISimpleTransform(header_, output_header_, false), expressions(std::move(expressions_))
{
}

void ExpandTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();

    /// step 1 : generate multiple blocks
    std::vector<Block> blocks;
    Columns columns = chunk.detachColumns();
    for (auto & expression : expressions)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(columns);
        expression->execute(block, chunk.getSideBlock(), num_rows);
        blocks.emplace_back(block);
    }

    // step 2 : check block header with output header, convert if necessary.
    Block output_block = getOutputPort().getHeader();
    for (auto & block : blocks)
    {
        if (!isCompatibleHeader(block, output_block))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                block.getColumnsWithTypeAndName(), output_block.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            converting_actions->execute(block, chunk.getSideBlock(), num_rows);
        }
    }

    // step 3 : merge expand blocks
    Block result = concatenateBlocks(blocks);

    chunk.setColumns(result.getColumns(), result.rows());
}

}
