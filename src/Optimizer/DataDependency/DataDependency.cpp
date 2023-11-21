#include <Optimizer/DataDependency/DataDependency.h>

namespace DB
{

DataDependency DataDependency::translate(const std::unordered_map<String, String> & identities) const
{
    return DataDependency{functional_dependencies.translate(identities), inclusion_dependency.translate(identities)};
}

DataDependency DataDependency::normalize(const SymbolEquivalences & symbol_equivalences) const
{
    return DataDependency{functional_dependencies.normalize(symbol_equivalences), inclusion_dependency.normalize(symbol_equivalences)};
}

}
