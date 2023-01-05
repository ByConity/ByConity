#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{
class EnforceSingleRowTransform : public IProcessor
{
public:
    explicit EnforceSingleRowTransform(const Block & header_);

    String getName() const override { return "EnforceSingleRowTransform"; }

    Status prepare() override;

private:
    InputPort & input;
    OutputPort & output;
    Port::Data single_row;

    bool has_input = false;
    bool output_finished = false;

    /**
     * create a row with all columns set null, if input don't have any results.
     */
    Port::Data createNullSingleRow() const;
};
}
