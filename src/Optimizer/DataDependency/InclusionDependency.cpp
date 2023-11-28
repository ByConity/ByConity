#include <algorithm>
#include <iterator>
#include <Optimizer/DataDependency/InclusionDependency.h>
#include <common/logger_useful.h>
#include "Optimizer/DataDependency/DependencyUtils.h"
#include <Core/Names.h>

namespace DB
{

InclusionDependency InclusionDependency::translate(const std::unordered_map<String, String> & identities) const
{
    InclusionDependency result;

    for (const auto & [cur_name, inclusion_element] : *this)
    {
        if (identities.contains(cur_name))
            result.emplace(identities.at(cur_name), inclusion_element);
    }

    return result;
}

InclusionDependency InclusionDependency::normalize(const SymbolEquivalences &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "InclusionDependency::normalize not implemented!");
}

}
