#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <Functions/FunctionsHashing.h>
#include <Optimizer/DataDependency/ForeignKeysTuple.h>
#include <Optimizer/DataDependency/FunctionalDependency.h>
#include <Optimizer/DataDependency/InclusionDependency.h>
#include <Optimizer/Property/Equivalences.h>
#include <Storages/ForeignKeysDescription.h>
namespace DB
{
class DataDependency;
using DataDependencyVector = std::vector<DataDependency>;
using DataDependencySets = std::vector<DataDependencyVector>;

class DataDependency
{
public:
    explicit DataDependency(const FunctionalDependencies & functional_dependencies_ = {}, const InclusionDependency & inclusion_dependency_ = {})
        : functional_dependencies(functional_dependencies_), inclusion_dependency(inclusion_dependency_)
    {
    }

    const FunctionalDependencies & getFunctionalDependencies() const
    {
        return functional_dependencies;
    }

    const InclusionDependency & getInclusionDependency() const
    {
        return inclusion_dependency;
    }

    FunctionalDependencies & getFunctionalDependenciesRef()
    {
        return functional_dependencies;
    }

    InclusionDependency & getInclusionDependencyRef()
    {
        return inclusion_dependency;
    }

    void setFunctionalDependencies(FunctionalDependencies functional_dependencies_)
    {
        functional_dependencies = std::move(functional_dependencies_);
    }

    void setInclusionDependency(InclusionDependency inclusion_dependency_)
    {
        inclusion_dependency = std::move(inclusion_dependency_);
    }

    DataDependency clearFunctionalDependency() const
    {
        return DataDependency{};
    }

    DataDependency translate(const std::unordered_map<String, String> & identities) const;
    DataDependency normalize(const SymbolEquivalences & symbol_equivalences) const;

    bool operator==(const DataDependency & other) const
    {
        return functional_dependencies == other.functional_dependencies;
    }

    bool operator!=(const DataDependency & other) const
    {
        return !(*this == other);
    }

private:
    FunctionalDependencies functional_dependencies;
    InclusionDependency inclusion_dependency;
};

}
