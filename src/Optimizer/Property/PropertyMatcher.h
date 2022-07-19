#pragma once

#include <Optimizer/Equivalences.h>
#include <Optimizer/Property/Property.h>

namespace DB
{
class PropertyMatcher
{
public:
    static bool matchNodePartitioning(
        const Context & context, Partitioning & required, const Partitioning & actual, const Equivalences & equivalences = {});
    static bool matchStreamPartitioning(
        const Context & context, const Partitioning & required, const Partitioning & actual, const Equivalences & equivalences = {});
    static Property compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & properties);
};
}
