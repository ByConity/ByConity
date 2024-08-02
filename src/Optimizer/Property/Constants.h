#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <Optimizer/Property/SymbolEquivalencesDeriver.h>

namespace DB
{

class Constants
{
public:
    Constants() = default;
    explicit Constants(std::map<String, FieldWithType> values_) : values(std::move(values_))
    {
    }
    const std::map<String, FieldWithType> & getValues() const
    {
        return values;
    }
    bool contains(const String & name) const
    {
        return values.contains(name);
    }

    Constants translate(const std::unordered_map<String, String> & identities) const;
    Constants normalize(const SymbolEquivalences & symbol_equivalences) const;
    String toString() const;

private:
    std::map<String, FieldWithType> values{};
};

using ConstantsSet = std::vector<Constants>;
}
