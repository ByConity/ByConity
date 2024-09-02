/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <optional>
#include <Optimizer/domain.h>

namespace DB::Predicate
{

Domain Domain::multipleValues(const DataTypePtr & type, const Array & values, bool null_allowed)
{
    if (values.empty())
        throw Exception("values cannot be empty", DB::ErrorCodes::LOGICAL_ERROR);

    return {createValueSet(type, values), null_allowed};
}

static Domain unionDomainsImpl(const Domains & domains, size_t cur_id) // NOLINT(misc-no-recursion)
{
    const auto & cur_domain = domains.at(cur_id);

    if (cur_id == 0)
        return cur_domain;
    else
        return cur_domain.unionn(unionDomainsImpl(domains, cur_id - 1));
}

Domain Domain::unionDomains(const Domains & domains)
{
    if (domains.empty())
        throw Exception("domains cannot be empty for union", DB::ErrorCodes::LOGICAL_ERROR);

    return unionDomainsImpl(domains, domains.size() - 1);
}

const Field & Domain::getSingleValue() const
{
    if (!isSingleValue())
        throw Exception("Domain is not a single value", DB::ErrorCodes::LOGICAL_ERROR);

    return std::visit([](auto & v) -> const Field & {return v.getSingleValue();}, value_set);
}

Field Domain::getNullableSingleValue() const
{
    if (!isNullableSingleValue())
        throw Exception("Domain is not a nullable single value", DB::ErrorCodes::LOGICAL_ERROR);

    if (null_allowed)
        return Null();

    return getSingleValue();
}

bool Domain::includesNullableValue(const Field & value) const
{
    return value.isNull() ? null_allowed : std::visit([&](auto & v) {return v.containsValue(value);}, value_set);
}

bool Domain::isNullableDiscreteSet() const
{
    auto caller = [](const auto & v)->bool
    {
        return v.isDiscreteSet();
    };
    return valueSetIsNone() ? null_allowed : std::visit(caller, value_set);
}

Array Domain::getNullableDiscreteSet() const
{
    if (!isNullableDiscreteSet())
        throw Exception("Domain is not a nullable discrete set", DB::ErrorCodes::LOGICAL_ERROR);

    Array res;

    if (!valueSetIsNone())
    {
        auto caller = [](const auto & v)
        {
            return v.getDiscreteSet();
        };
        res = std::visit(caller, value_set);
    }

    if (null_allowed)
        res.emplace_back(Null());

    return res;
}

Domain Domain::intersect(const Domain & other) const
{
    bool res_null_allowed = null_allowed && other.isNullAllowed();
    auto res_value_set = visitOnSameType([](auto & v1, auto & v2) -> ValueSet
                                         {
                                             return v1.intersect(v2);
                                         }, other.getValueSet());
    return {std::move(res_value_set), res_null_allowed};
}

Domain Domain::unionn(const Domain & other) const
{
    bool res_null_allowed = null_allowed || other.isNullAllowed();
    auto res_value_set = visitOnSameType([](auto & v1, auto & v2) -> ValueSet
                                         {
                                             return v1.unionn(v2);
                                         }, other.getValueSet());
    return {std::move(res_value_set), res_null_allowed};
}

Domain Domain::complement() const
{
    auto caller = [](const auto & v)->ValueSet
    {
        return v.complement();
    };

    return {std::visit(caller, value_set), !null_allowed};
}

Domain Domain::subtract(const Domain & other)
{
    bool res_null_allowed = null_allowed && !other.isNullAllowed();
    auto res_value_set = visitOnSameType([](auto & v1, auto & v2) -> ValueSet
                                         {
                                             return v1.subtract(v2);
                                         }, other.getValueSet());
    return {std::move(res_value_set), res_null_allowed};
}

bool Domain::overlaps(const Domain & other) const
{
    if (isNullAllowed() && other.isNullAllowed())
        return true;

    return visitOnSameType([](auto & v1, auto & v2) -> bool
                           {
                               return v1.overlaps(v2);
                           }, other.getValueSet());
}

bool Domain::contains(const Domain & other) const
{
    if (!isNullAllowed() && other.isNullAllowed()) {
        return false;
    }

    return visitOnSameType([](auto & v1, auto & v2) -> bool
                           {
                               return v1.contains(v2);
                           }, other.getValueSet());
}

bool Domain::operator==(const Domain & other) const
{
    if (null_allowed != other.isNullAllowed())
        return false;

    return visitOnSameType([](auto & v1, auto & v2) -> bool
                           {
                               return v1 == v2;
                           }, other.getValueSet());
}

String Domain::toString() const
{
    auto value_set_to_str = std::visit([](const auto & v) -> String { return v.toString(); }, value_set);
    return value_set_to_str + (null_allowed ? " NULL" : " NOTNULL");
}

template <typename T, typename Hash, typename Equal>
TupleDomainImpl<T, Hash, Equal>::TupleDomainImpl(DomainMap domains_) : is_none(false), domains(std::move(domains_))
{
    for (auto it = domains.begin(); it != domains.end();)
    {
        if (it->second.isNone())
        {
            is_none = true;
            break;
        }

        if (it->second.isAll())
            it = domains.erase(it);
        else
            ++it;
    }

    if (is_none)
        domains.clear();
}


///Extract all column constraints that require exactly one value or only null in their respective Domains.
///Returns an empty Optional if the Domain is none or all.
template <typename T, typename Hash, typename Equal>
std::optional<typename TupleDomainImpl<T, Hash, Equal>::FieldWithTypeMap> TupleDomainImpl<T, Hash, Equal>::extractFixedValues() const
{
    //if tuple_domain is "none" or is "all"
    if (domainsIsEmpty())
        return std::nullopt;

    TupleDomainImpl<T, Hash, Equal>::FieldWithTypeMap single_values;
    for (const auto & domain : domains)
    {
        if (domain.second.isNullableSingleValue())
        {
            single_values.emplace(domain.first, FieldWithType{domain.second.getType(), domain.second.getNullableSingleValue()});
        }
    }
    return single_values;
}

///Extract all column constraints that define a non-empty set of discrete values allowed for the columns in their respective Domains.
///Returns an empty Optional if the Domain is none or all.
template <typename T, typename Hash, typename Equal>
std::optional<LinkedHashMap<T, Array, Hash, Equal>> TupleDomainImpl<T, Hash, Equal>::extractDiscreteValues() const
{
    //if tuple_domain is "none" or is "all"
    if (domainsIsEmpty())
        return std::nullopt;

    LinkedHashMap<T, Array, Hash, Equal> discrete_values_map;
    for (const auto & domain : domains)
    {
        if (domain.second.isNullableDiscreteSet())
        {
            discrete_values_map.emplace(domain.first, domain.second.getNullableDiscreteSet());
        }
    }
    return discrete_values_map;
}

///Convert a map of columns to values into the TupleDomain which requires
///those columns to be fixed to those values. Null is allowed as a fixed value.
template <typename T, typename Hash, typename Equal>
TupleDomainImpl<T, Hash, Equal>
TupleDomainImpl<T, Hash, Equal>::fromFixedValues(const TupleDomainImpl<T, Hash, Equal>::FieldWithTypeMap & fixed_values)
{
    DomainMap domains;
    for (const auto & item : fixed_values)
    {
        const FieldWithType & type_and_field = item.second;
        domains.emplace(item.first,
                        type_and_field.value.isNull() ? Domain::onlyNull(type_and_field.type) : Domain::singleValue(type_and_field.type, type_and_field.value));
    }
    return TupleDomainImpl(domains);
}

///Returns the strict intersection of the TupleDomains.
///The resulting TupleDomain represents the set of tuples that would be valid in both TupleDomains.
template <typename T, typename Hash, typename Equal>
TupleDomainImpl<T, Hash, Equal> TupleDomainImpl<T, Hash, Equal>::intersect(const std::vector<TupleDomainImpl<T, Hash, Equal>> & others)
{
    if (others.empty())
        return all();
    if (others.size() == 1)
        return others[0];

    std::vector<TupleDomainImpl<T, Hash, Equal>> candidate;
    bool all_equals = true;
    for (const auto & tuple_domain : others)
    {
        if (tuple_domain.isNone())
            return none();

        if (all_equals && !(others[0] == tuple_domain))
            all_equals = false;

        if (!tuple_domain.isAll())
            candidate.emplace_back(tuple_domain);
    }

    if (candidate.empty())
        return all();

    if (all_equals || candidate.size() == 1)
        return candidate[0];

    DomainMap root_domains = candidate[0].getDomains();
    for (size_t i = 1; i < candidate.size(); i++)
    {
        for (const auto & domains_ref : candidate[i].getDomains())
        {
            if (!root_domains.count(domains_ref.first))
            {
                root_domains.emplace(domains_ref.first, domains_ref.second);
            }
            else
            {
                Domain intersect_domains = root_domains.at(domains_ref.first).intersect(domains_ref.second);
                if (intersect_domains.isNone())
                    return none();
                root_domains.at(domains_ref.first) = std::move(intersect_domains);
            }
        }
    }
    return TupleDomainImpl(root_domains);
}

/**
  * Returns a TupleDomain which contains values in self set but not in other,
  * it looks like difference of two set in mathematics, or std::set_difference in c++.
  *
  * <p>
  * eg, input: Tuple(Domain[A] ∩ Domain[B]), other: Tuple(Domain'[B] ∩ Domain[C])
  * result: Tuple(Domain[A] ∩ Domain[B]) - Tuple(Domain[A] ∩ Domain[B])
  *       = Domain[A] ∩ Domain[B] ∩ (^Domain'[B] ∪ ^Domain[C])
  *
  * <p>
  * Return std::nullopt if the result is dnf, as dnf is useless for pruning.
  *
  * <p>
  * In this case, if we assume ^Domain'[B] is subset of Domain[B], the result is TupleDomain(Domain[A] ∩ Domain[B] ∩ ^Domain[C]).
  */
template <typename T, typename Hash, typename Equal>
std::optional<TupleDomainImpl<T, Hash, Equal>>
TupleDomainImpl<T, Hash, Equal>::subtract(const TupleDomainImpl<T, Hash, Equal> & other) const
{
    if (this->isNone())
        return none();
    if (other.isAll())
        return none();
    if (other.isNone())
        return {*this};

    DomainMap domain_map;
    for (const auto & other_domain : other.getDomains())
    {
        auto complement = other_domain.second.complement();
        if (this->getDomains().contains(other_domain.first))
        {
            const auto & self_domain = this->getDomains().at(other_domain.first);
            complement = self_domain.intersect(complement);
            if (complement.isNone())
                continue;
        }

        if (!domain_map.empty())
            return std::nullopt;

        domain_map.emplace(other_domain.first, complement);
    }
    return TupleDomainImpl{domain_map};
}

/// Returns the tuple domain that contains all other tuple domains, or {@code std::nullopt} if they are not supersets of each other.
template <typename T, typename Hash, typename Equal>
std::optional<TupleDomainImpl<T, Hash, Equal>>
TupleDomainImpl<T, Hash, Equal>::maximal(const std::vector<TupleDomainImpl<T, Hash, Equal>> & domains)
{
    if (domains.empty())
        return std::nullopt;

    size_t largest_idx = 0;

    for (size_t i = 1; i < domains.size(); i ++)
    {
        const auto & largest = domains[largest_idx];
        const auto & current = domains[i];

        if (current.contains(largest))
        {
            largest_idx = i;
        }
        else if (!largest.contains(current))
        {
            return std::nullopt;
        }
    }
    return domains[largest_idx];
}

///Returns true only if the this TupleDomain contains all possible tuples that would be allowable by the other TupleDomain.
template <typename T, typename Hash, typename Equal>
bool TupleDomainImpl<T, Hash, Equal>::contains(const TupleDomainImpl<T, Hash, Equal> & other) const
{
    if (other.isNone())
        return true;

    if (isNone())
        return false;

    for (const auto & this_domain : domains)
    {
        bool if_hava_the_domain = other.haveSpecificDomain(this_domain.first);
        if (!if_hava_the_domain || !this_domain.second.contains(other.getDomains().at(this_domain.first)))
            return false;
    }
    return true;
}

/**
     * Returns a TupleDomain in which corresponding column Domains are unioned together.
     * <p>
     * Note that this is NOT equivalent to a strict union as the final result may allow tuples
     * that do not exist in either TupleDomain.
     * Example 1:
     * <p>
     * <ul>
     * <li>TupleDomain X: a => 1, b => 2
     * <li>TupleDomain Y: a => 2, b => 3
     * <li>Column-wise unioned TupleDomain: a => 1 OR 2, b => 2 OR 3
     * </ul>
     * <p>
     * In the above resulting TupleDomain, tuple (a => 1, b => 3) would be considered valid but would
     * not be valid for either TupleDomain X or TupleDomain Y.
     * Example 2:
     * <p>
     * Let a be of type DOUBLE
     * <ul>
     * <li>TupleDomain X: (a < 5)
     * <li>TupleDomain Y: (a > 0)
     * <li>Column-wise unioned TupleDomain: (a IS NOT NULL)
     * </ul>
     * </p>
     * In the above resulting TupleDomain, tuple (a => NaN) would be considered valid but would
     * not be valid for either TupleDomain X or TupleDomain Y.
     * However, this result is guaranteed to be a superset of the strict union.
     */
template <typename T, typename Hash, typename Equal>
TupleDomainImpl<T, Hash, Equal>
TupleDomainImpl<T, Hash, Equal>::columnWiseUnion(const std::vector<TupleDomainImpl<T, Hash, Equal>> & tuple_domains)
{
    if (tuple_domains.empty()) {
        throw Exception("tuple_domains must have at least one element", DB::ErrorCodes::LOGICAL_ERROR);
    }

    if (tuple_domains.size() == 1) {
        return tuple_domains[0];
    }

    // gather all common columns
    std::unordered_set<T, Hash, Equal> common_columns;

    // first, find a non-none domain
    bool found = false;
    size_t index = 0;
    for (; index < tuple_domains.size(); index++)
    {
        const TupleDomainImpl<T, Hash, Equal> & temp = tuple_domains[index];
        if (temp.isAll())
            return all();

        if(!temp.isNone())
        {
            found = true;
            for (const auto & domain : temp.getDomains())
            {
                common_columns.emplace(domain.first);
            }
            break;
        }
    }

    if (!found) {
        return none();
    }

    for (; index < tuple_domains.size(); index++)
    {
        if (!tuple_domains[index].isNone())
        {
            auto it = common_columns.begin();
            while (it != common_columns.end())
            {
                if (!tuple_domains[index].haveSpecificDomain(*it))
                {
                    common_columns.erase(it++);
                }
                else
                {
                    it++;
                }
            }
        }
    }
    // group domains by column (only for common columns)
    std::unordered_map<T, std::vector<Domain>, Hash, Equal> domains_by_column;
    for (const auto & tuple_domain_ref : tuple_domains)
    {
        if (!tuple_domain_ref.isNone())
        {
            for (const auto & domain : tuple_domain_ref.getDomains())
            {
                if (common_columns.count(domain.first))
                {
                    if (!domains_by_column.count(domain.first))
                    {
                        domains_by_column.insert(std::make_pair(domain.first, std::vector<Domain>{domain.second}));
                        continue;
                    }
                    domains_by_column.at(domain.first).emplace_back(domain.second);
                }
            }
        }
    }
    // finally, do the column-wise union
    DomainMap result;
    for (const auto & multiple_domains : domains_by_column) {
        result.emplace_back(std::make_pair(multiple_domains.first, Domain::unionDomains((multiple_domains.second))));
    }
    return TupleDomainImpl(result);
}

/**
     * Returns true only if there exists a strict intersection between the TupleDomains.
     * i.e. there exists some potential tuple that would be allowable in both TupleDomains.
     */
template <typename T, typename Hash, typename Equal>
bool TupleDomainImpl<T, Hash, Equal>::overlaps(const TupleDomainImpl<T, Hash, Equal> & other) const
{
    if (isNone() || other.isNone()) {
        return false;
    }
    if (isAll() || other.isAll()) {
        return true;
    }

    for (const auto & other_domain : other.getDomains())
    {
        if (domains.count(other_domain.first))
        {
            if (!domains.at(other_domain.first).overlaps(other_domain.second))
                return false;
        }
    }
    // All the common columns have overlapping domains
    return true;
}

template class TupleDomainImpl<String, std::hash<String>, std::equal_to<String>>;
template class TupleDomainImpl<ASTPtr, ASTEquality::ASTHash, ASTEquality::ASTEquals>;
}
