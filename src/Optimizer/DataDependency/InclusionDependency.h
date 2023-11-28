#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Optimizer/DataDependency/DependencyUtils.h>
#include <Optimizer/Property/Equivalences.h>

namespace DB
{
using SymbolEquivalences = Equivalences<String>;

using InclusionElement = std::pair<bool, String>;

// inclusion relationship:
// element for fk: current_name -> {true, related_pk_full_name}
// element for pk: current_name -> {false, pk_full_name(tbl_name.column_name)}
class InclusionDependency : public std::unordered_map<String, InclusionElement>
{
public:
    InclusionDependency() = default;

    InclusionDependency operator|(const InclusionDependency & other) const
    {
        InclusionDependency result = *this;
        result.insert(other.begin(), other.end());
        return result;
    }

    String string() const
    {
        std::ostringstream ostr;
        for (const auto & [cur_name, inclusion_element] : *this)
        {
            ostr << "\n\tcur_name=" + cur_name;
            ostr << ", is_included=" + std::to_string(inclusion_element.first) + ", original_name=" + inclusion_element.second;
        }
        return ostr.str();
    }


    InclusionDependency translate(const std::unordered_map<String, String> & identities) const;
    InclusionDependency normalize(const SymbolEquivalences & symbol_equivalences) const;
};

}
