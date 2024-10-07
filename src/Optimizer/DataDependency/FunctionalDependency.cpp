#include <algorithm>
#include <iterator>
#include <Optimizer/DataDependency/FunctionalDependency.h>
#include <common/logger_useful.h>
#include "Optimizer/DataDependency/DependencyUtils.h"
#include <Core/Names.h>

namespace DB
{

void FunctionalDependencies::update(const FunctionalDependency & dependency_)
{
    auto & dependents = dependencies[dependency_.first];
    // TODO@lijinzhi.zx: merge dependents with dependency_.second.
    dependents.insert(dependency_.second);
}

// group by A, B, C <=> group by A when A -> B, C.
NameSet FunctionalDependencies::simplify(NameSet srcs) const
{
    if (srcs.empty())
        return srcs;

    std::string str;
    for (const auto & name : srcs)
        str += name + ",";
    LOG_INFO(getLogger("DataDependency"), "FunctionalDependencies::simplify srcs -- " + str);

    // LOG_INFO(getLogger("DataDependency"), "FDS: " + string());

    for (const auto & [determinant, dependents] : dependencies)
    {
        // if srcs(e.g. A, B, C, D) contains A.
        // and has FDS: A->B, A->C.
        // then srcs can be simplified as A, D.
        if (isSubNameSet(determinant, srcs))
        {
            size_t can_simplified_size = 0;
            for (const auto & src : srcs)
            {
                if (!determinant.contains(src) && dependents.contains({src}))
                    ++can_simplified_size;
            }
            if (can_simplified_size > 0)
            {
                std::erase_if(srcs, [dependents = dependents](const String & src){ return dependents.contains({src}); });
                break;
            }
        }
    }

    return srcs; // can't simplify
}

void FunctionalDependencies::eraseNotExist(const Names & /*names_*/)
{
    // TODO
}

FunctionalDependencies FunctionalDependencies::translate(const std::unordered_map<String, String> & identities) const
{
    FunctionalDependencies result;
    auto translate_names = [&](const NameSet & names) {
        NameSet res;
        for (const String & name : names)
        {
            if (identities.contains(name))
                res.emplace(identities.at(name));
        }
        return res;
    };

    for (const auto & [determinant, dependents] : dependencies)
    {
        auto translated_determinant = translate_names(determinant);
        if (translated_determinant.size() != determinant.size())
            continue;
        auto & new_dependents = result.dependencies[translated_determinant];
        for (const auto & dependent : dependents)
            if (auto translated_dependent = translate_names(dependent); !translated_dependent.empty())
                new_dependents.insert(translated_dependent);
    }

    return result;
}

FunctionalDependencies FunctionalDependencies::normalize(const SymbolEquivalences & symbol_equivalences) const
{
    auto mapping = symbol_equivalences.representMap();
    auto normalize_names = [&](const NameSet & names) {
        for (const String & name : names)
        {
            if (!mapping.contains(name))
            {
                mapping[name] = name;
            }
        }
    };

    for (const auto & [k, v] : dependencies)
    {
        normalize_names(k);
        for (const auto & dependent : v)
            normalize_names(dependent);
    }
    return translate(mapping);
}

}
