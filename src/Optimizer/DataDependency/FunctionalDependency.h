#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Optimizer/DataDependency/DependencyUtils.h>
#include <Optimizer/Property/Equivalences.h>

namespace DB
{
using SymbolEquivalences = Equivalences<String>;

using FunctionalDependency = std::pair<NameSet, NameSet>;
class FunctionalDependencies
{
public:
    FunctionalDependencies() = default;

    void update(const FunctionalDependency & dependency_);
    void eraseNotExist(const Names & names);

    NameSet simplify(NameSet srcs) const;

    bool operator==(const FunctionalDependencies & other) const
    {
        return dependencies == other.dependencies;
    }

    FunctionalDependencies operator|(const FunctionalDependencies & other) const
    {
        FunctionalDependencies result = *this;
        for (const auto & [determinant, dependents] : other.dependencies)
        {
            for (const auto & dependent : dependents)
                result.update({determinant, dependent});
        }
        return result;
    }

    String string() const
    {
        std::ostringstream ostr;
        for (const auto & dependency : dependencies)
        {
            ostr << "\n\tdeterminant-";
            for (const auto & str : dependency.first)
                ostr << str << ",";
            ostr << "dependent-";
            for (const auto & dependents : dependency.second)
            {
                ostr << "[";
                for (const auto & dependent : dependents)
                    ostr << dependent << ",";
                ostr << "], ";
            }
        }
        return ostr.str();
    }


    FunctionalDependencies translate(const std::unordered_map<String, String> & identities) const;
    FunctionalDependencies normalize(const SymbolEquivalences & symbol_equivalences) const;

private:
    struct HashNameSet 
    {
    public:
        std::size_t operator()(const NameSet & name_set) const
        {
            size_t hash = 0;
            for (const auto & name : name_set)
                hash ^= std::hash<std::string>()(name);
            return hash;
        }
    };
    /// determinant -> dependents
    std::unordered_map<NameSet, std::unordered_set<NameSet, HashNameSet>, HashNameSet> dependencies;
};

}
