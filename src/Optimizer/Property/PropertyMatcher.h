#pragma once

#include <Optimizer/Property/Property.h>
#include <Optimizer/SymbolEquivalencesDeriver.h>

namespace DB
{
class PropertyMatcher
{
public:
    static bool matchNodePartitioning(
        const Context & context, Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences = {});

    static bool matchStreamPartitioning(
        const Context & context,
        const Partitioning & required,
        const Partitioning & actual,
        const SymbolEquivalences & equivalences = {});
    static Property compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & properties);
};
}
