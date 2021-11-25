#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class SubstitutionTransform : public ISimpleTransform
{
public:
    SubstitutionTransform(const Block & header_, const std::unordered_map<String, String> & name_substitution_info_);

    String getName() const override { return "SubstitutionTransform"; }

    static Block transformHeader(Block header, const std::unordered_map<String, String> & name_substitution_info_);

protected:
    void transform(Chunk & chunk) override;

private:
    const std::unordered_map<String, String> & name_substitution_info;
};

}
