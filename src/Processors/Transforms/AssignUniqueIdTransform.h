#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{
class AssignUniqueIdTransform : public ISimpleTransform
{
public:
    AssignUniqueIdTransform(const Block & header_, String symbol_);

    String getName() const override { return "AssignUniqueIdTransform"; }
    static Block transformHeader(Block header, String symbol);

protected:
    void transform(Chunk & chunk) override;

private:
    static Int64 ROW_IDS_PER_REQUEST;
    static Int64 MAX_ROW_ID;
    String symbol;
    std::atomic<Int64> row_id_pool{0};
    Int64 unique_value_mask;
    Int64 row_id_counter{0};
    Int64 max_row_id_counter_value{0};
    void requestValues();
};
}
