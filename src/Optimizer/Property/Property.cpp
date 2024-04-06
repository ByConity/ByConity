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

#include <Optimizer/Property/Property.h>

#include <Functions/FunctionsHashing.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Optimizer/Property/SymbolEquivalencesDeriver.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/SymbolEquivalencesDeriver.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/PlanSerDerHelper.h>

namespace DB
{
size_t Partitioning::hash() const
{
    size_t hash = IntHash64Impl::apply(static_cast<UInt8>(handle));

    hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(columns.size()));
    for (const auto & column : columns)
        hash = MurmurHash3Impl64::combineHashes(hash, MurmurHash3Impl64::apply(column.c_str(), column.size()));

    hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(require_handle));
    hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(buckets));
    hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(enforce_round_robin));
    return hash;
}

bool Partitioning::operator==(const Partitioning & other) const
{
    return handle == other.handle && columns == other.columns && require_handle == other.require_handle && buckets == other.buckets
        && enforce_round_robin == other.enforce_round_robin && isRequireHandle() == other.isRequireHandle();
}

bool Partitioning::satisfy(const Partitioning & requirement, const Constants & constants) const
{
    if (requirement.require_handle)
        return getPartitioningHandle() == requirement.getPartitioningHandle() && getBuckets() == requirement.getBuckets()
            && getPartitioningColumns() == requirement.getPartitioningColumns();

    switch (requirement.component)
    {
        case Component::COORDINATOR: {
            if (component == Component::WORKER)
            {
                return false;
            }
            break;
        }
        case Component::WORKER: {
            if (component == Component::COORDINATOR)
            {
                return false;
            }
            break;
        }
        default:
            break;
    }

    switch (requirement.getPartitioningHandle())
    {
        case Handle::FIXED_HASH:
            return getPartitioningColumns() == requirement.getPartitioningColumns()
                || (!requirement.isExactlyMatch() && this->isPartitionOn(requirement, constants));
        default:
            return getPartitioningHandle() == requirement.getPartitioningHandle() && getBuckets() == requirement.getBuckets()
                && getPartitioningColumns() == requirement.getPartitioningColumns();
    }
}

bool Partitioning::isPartitionOn(const Partitioning & requirement, const Constants & constants) const
{
    auto actual_columns = getPartitioningColumns();
    auto required_columns = requirement.getPartitioningColumns();
    std::unordered_set<std::string> required_columns_set;

    if (actual_columns.empty())
        return false;

    for (auto & required_column : required_columns)
    {
        required_columns_set.insert(required_column);
    }

    for (auto & actual_column : actual_columns)
    {
        if (constants.contains(actual_column))
            continue;

        if (!required_columns_set.count(actual_column))
        {
            return false;
        }
    }

    return true;
}

Partitioning Partitioning::normalize(const SymbolEquivalences & symbol_equivalences) const
{
    auto mapping = symbol_equivalences.representMap();
    for (const auto & item : columns)
    {
        if (!mapping.contains(item))
        {
            mapping[item] = item;

            // if (!output_symbols.contains(item))
            // {
            //     return Partitioning{};
            // }
        }
    }
    return this->translate(mapping);
}

Partitioning Partitioning::translate(const std::unordered_map<String, String> & identities) const
{
    Names translate_columns;
    for (const auto & column : columns)
    {
        if (identities.contains(column))
            translate_columns.emplace_back(identities.at(column));
        else // note: don't discard column
            translate_columns.emplace_back(column);
    }
    return Partitioning{handle, translate_columns, require_handle, buckets, enforce_round_robin, component, exactly_match, satisfy_worker};
}


void Partitioning::toProto(Protos::Partitioning & proto) const
{
    proto.set_handle(Partitioning::HandleConverter::toProto(handle));
    for (const auto & element : columns)
        proto.add_columns(element);
    proto.set_require_handle(require_handle);
    proto.set_buckets(buckets);
    proto.set_enforce_round_robin(enforce_round_robin);
    proto.set_component(Partitioning::ComponentConverter::toProto(component));
    proto.set_exactly_match(exactly_match);
}

Partitioning Partitioning::fromProto(const Protos::Partitioning & proto)
{
    auto handle = Partitioning::HandleConverter::fromProto(proto.handle());
    std::vector<String> columns;
    for (const auto & element : proto.columns())
        columns.emplace_back(element);
    auto require_handle = proto.require_handle();
    auto buckets = proto.buckets();
    auto enforce_round_robin = proto.enforce_round_robin();
    auto component = Partitioning::ComponentConverter::fromProto(proto.component());
    auto exactly_match = proto.exactly_match();
    return Partitioning(handle, columns, require_handle, buckets, enforce_round_robin, component, exactly_match);
}

String Partitioning::toString() const
{
    switch (handle)
    {
        case Handle::SINGLE:
            return "SINGLE";
        case Handle::COORDINATOR:
            return "COORDINATOR";
        case Handle::FIXED_HASH:
            if (columns.empty())
                return "[]";
            else
            {
                auto result = "["
                    + std::accumulate(
                                  std::next(columns.begin()),
                                  columns.end(),
                                  columns[0],
                                  [](String a, const String & b) { return std::move(a) + ", " + b; })
                    + "]";
                if (enforce_round_robin)
                    result += "RR";
                if (exactly_match)
                    result += " EM";
                return result;
            }
        case Handle::FIXED_ARBITRARY:
            return "FIXED_ARBITRARY";
        case Handle::FIXED_BROADCAST:
            return "BROADCAST";
        case Handle::SCALED_WRITER:
            return "SCALED_WRITER";
        case Handle::BUCKET_TABLE:
            return "BUCKET_TABLE";
        case Handle::ARBITRARY:
            return "ARBITRARY";
        case Handle::FIXED_PASSTHROUGH:
            return "FIXED_PASSTHROUGH";
        default:
            return "UNKNOWN";
    }
}

size_t SortColumn::hash() const
{
    size_t hash = MurmurHash3Impl64::apply(name.c_str(), name.size());
    hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(static_cast<UInt8>(order)));
    return hash;
}

String SortColumn::toString() const
{
    switch (order)
    {
        case SortOrder::ASC_NULLS_FIRST:
            return name + "↑↑";
        case SortOrder::ASC_NULLS_LAST:
            return name + "↑↓";
        case SortOrder::DESC_NULLS_FIRST:
            return name + "↓↑";
        case SortOrder::DESC_NULLS_LAST:
            return name + "↓↓";
        case SortOrder::ANY:
            return name + "any";
        case SortOrder::UNKNOWN:
            return name + "unknown";
    }
    return "unknown";
}

size_t Sorting::hash() const
{
    size_t hash = IntHash64Impl::apply(this->size());
    for (const auto & item : *this)
        hash = MurmurHash3Impl64::combineHashes(hash, item.hash());
    return hash;
}

Sorting Sorting::translate(const std::unordered_map<String, String> & identities) const
{
    Sorting result;
    for (const auto & item : *this)
        if (identities.contains(item.getName()))
            result.emplace_back(SortColumn{identities.at(item.getName()), item.getOrder()});
        else
            result.emplace_back(item);
    return result;
}

Sorting Sorting::normalize(const SymbolEquivalences & symbol_equivalences) const
{
    auto mapping = symbol_equivalences.representMap();
    for (const auto & item : *this)
    {
        if (!mapping.contains(item.getName()))
        {
            mapping[item.getName()] = item.getName();

            // if (!output_symbols.contains(item.getName()))
            // {
            //     return Sorting{};
            // }
        }
    }
    return translate(mapping);
}

String Sorting::toString() const
{
    return std::accumulate(
        std::next(begin()), end(), front().toString(), [](std::string a, const auto & b) { return std::move(a) + '-' + b.toString(); });
}

size_t CTEDescriptions::hash() const
{
    size_t hash = IntHash64Impl::apply(cte_descriptions.size());
    for (const auto & item : cte_descriptions)
    {
        hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(item.first));
        hash = MurmurHash3Impl64::combineHashes(hash, item.second.hash());
    }
    hash = MurmurHash3Impl64::combineHashes(hash, explored.size());
    for (const auto & item : explored)
        hash = MurmurHash3Impl64::combineHashes(hash, item);
    return hash;
}

String CTEDescription::toString() const
{
    std::stringstream output;
    output << node_partitioning.toString();
    output << (preferred ? "?" : "");
    return output.str();
}

String CTEDescriptions::toString() const
{
    if (explored.empty())
        return "";
    size_t count = 0;
    std::stringstream output;
    for (const auto & cte_id : explored)
    {
        if (count++ > 0)
            output << " ";
        if (cte_descriptions.contains(cte_id))
            output << "CTE(" << cte_id << ")=" << cte_descriptions.at(cte_id).toString();
        else
            output << "CTE(" << cte_id << ")=inlined";
    }
    return output.str();
}

size_t TableLayout::hash() const
{
    size_t hash = IntHash64Impl::apply(this->size());
    for (const auto & item : *this)
    {
        hash = MurmurHash3Impl64::combineHashes(hash, item.first.hash());
        hash = MurmurHash3Impl64::combineHashes(hash, item.second.isStarPartitioned() ? 0 : item.second.getPartitionKey().hash());
    }
    return hash;
}

String TableLayout::toString() const
{
    auto it = begin();
    if (it == end())
        return "";
    std::stringstream output;
    output << "TableLayout(" << it->first.database << "." << it->first.table << ")="
           << (it->second.isStarPartitioned() ? "*" : it->second.getPartitionKey().column);
    while (++it != end())
        output << ","
               << "TableLayout(" << it->first.database << "." << it->first.table << ")="
               << (it->second.isStarPartitioned() ? "*" : it->second.getPartitionKey().column);
    return output.str();
}

Property Property::translate(const std::unordered_map<String, String> & identities) const
{
    Property result{node_partitioning.translate(identities), stream_partitioning.translate(identities), sorting.translate(identities)};
    result.setPreferred(preferred);
    result.setCTEDescriptions(cte_descriptions.translate(identities));
    return result;
}

Property Property::normalize(const SymbolEquivalences & symbol_equivalences) const
{
    Property result{
        node_partitioning.normalize(symbol_equivalences),
        stream_partitioning.normalize(symbol_equivalences),
        sorting.normalize(symbol_equivalences)};
    result.setPreferred(preferred);
    result.setCTEDescriptions(cte_descriptions);
    return result;
}

size_t Property::hash() const
{
    size_t hash = IntHash64Impl::apply(preferred);
    hash = MurmurHash3Impl64::combineHashes(hash, node_partitioning.hash());
    hash = MurmurHash3Impl64::combineHashes(hash, stream_partitioning.hash());
    hash = MurmurHash3Impl64::combineHashes(hash, sorting.hash());
    hash = MurmurHash3Impl64::combineHashes(hash, cte_descriptions.hash());
    hash = MurmurHash3Impl64::combineHashes(hash, table_layout.hash());
    return hash;
}

String Property::toString() const
{
    std::stringstream output;
    output << node_partitioning.toString();
    if (stream_partitioning.getPartitioningHandle() != Partitioning::Handle::ARBITRARY)
        output << "/" << stream_partitioning.toString();
    if (preferred)
        output << "?";
    if (!sorting.empty())
        output << " " << sorting.toString();
    if (!cte_descriptions.empty())
        output << " " << cte_descriptions.toString();
    if (!table_layout.empty())
        output << " " << table_layout.toString();
    return output.str();
}

void CTEDescriptions::filter(const std::unordered_set<CTEId> & allowed)
{
    std::map<CTEId, CTEDescription> filter_cte_descriptions;
    std::set<CTEId> filter_explored;
    for (const auto & item : cte_descriptions)
        if (allowed.contains(item.first))
            filter_cte_descriptions.emplace(item);
    for (const auto & item : explored)
        if (allowed.contains(item))
            filter_explored.emplace(item);
    cte_descriptions.swap(filter_cte_descriptions);
    explored.swap(filter_explored);
}

size_t CTEDescription::hash() const
{
    size_t hash = node_partitioning.hash();
    hash = MurmurHash3Impl64::combineHashes(hash, preferred);
    return hash;
}

CTEDescription::CTEDescription(const Property & property)
    : CTEDescription(property.getNodePartitioning(), property.isPreferred())
{
}

bool CTEDescription::operator==(const CTEDescription & other) const
{
    return node_partitioning == other.node_partitioning && preferred == other.preferred;
}

Property CTEDescription::createCTEDefGlobalProperty(const Property & property, CTEId cte_id)
{
    if (!property.getCTEDescriptions().isShared(cte_id))
    {
        Property any;
        any.setCTEDescriptions(property.getCTEDescriptions());
        any.getCTEDescriptions().erase(cte_id);
        return any;
    }

    const auto & cte_description = property.getCTEDescriptions().getSharedDescription(cte_id);
    // no need to translate.
    Property res{cte_description.node_partitioning};
    res.setPreferred(cte_description.preferred);
    // copy other cte descriptions.
    res.setCTEDescriptions(property.getCTEDescriptions());
    res.getCTEDescriptions().erase(cte_id);
    return res;
}

Property CTEDescription::createCTEDefLocalProperty(
    const Property & property, CTEId cte_id, const std::unordered_map<String, String> & identities_mapping)
{
    auto res = property.translate(identities_mapping);
    res.getCTEDescriptions().erase(cte_id);
    return res;
}

CTEDescription CTEDescription::translate(const std::unordered_map<String, String> & identities) const
{
    return CTEDescription{node_partitioning.translate(identities), preferred};
}

CTEDescriptions CTEDescriptions::translate(const std::unordered_map<String, String> & identities) const
{
    CTEDescriptions result;
    for (const auto & item : cte_descriptions)
        result.cte_descriptions.emplace(item.first, item.second.translate(identities));
    for (const auto & item : explored)
        result.explored.emplace(item);
    return result;
}

bool CTEDescriptions::operator==(const CTEDescriptions & rhs) const
{
    return cte_descriptions == rhs.cte_descriptions && explored == rhs.explored;
}


}
