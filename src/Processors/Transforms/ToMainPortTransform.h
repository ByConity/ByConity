#pragma once

#include <string>
#include <Processors/IProcessor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ISink.h>


namespace DB
{

/**
 * This Transform is responsible for transfering data from totals_port to main_port
*/
class TotalsPortToMainPortTransform : public ISimpleTransform
{
protected:
    void transform(Chunk &) override;

public:
    explicit TotalsPortToMainPortTransform(const Block & header_);
    String getName() const override
    {
        return "TotalsPortToMainPortTransform";
    }
};

/**
 * This Transform is responsible for transfering data from extremes_port to main_port
*/
class ExtremesPortToMainPortTransform : public ISimpleTransform
{
protected:
    void transform(Chunk &) override;

public:
    explicit ExtremesPortToMainPortTransform(const Block & header_);
    String getName() const override
    {
        return "ExtremesPortToMainPortTransform";
    }
};

}
