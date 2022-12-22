#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

class RollupWithGroupingTransform : public IAccumulatingTransform
{
public:
    RollupWithGroupingTransform(Block header, AggregatingTransformParamsPtr params, ColumnNumbers groupings_, Names grouping_names_);
    String getName() const override { return "RollupWithGroupingTransform"; }
    static Block transformHeader(Block res, const Names & grouping_names);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    ColumnNumbers keys;
    ColumnNumbers groupings;
    Names grouping_names;
    Chunks consumed_chunks;
    Chunk rollup_chunk;
    size_t last_removed_key = 0;

    Chunk merge(Chunks && chunks, bool final);
};

}
