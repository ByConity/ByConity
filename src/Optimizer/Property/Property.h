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

#pragma once

#include <unordered_map>
#include <Analyzers/ASTEquals.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <Functions/FunctionsHashing.h>
#include <Optimizer/FunctionInvoker.h>
#include <Optimizer/Property/Equivalences.h>
#include <Parsers/IAST_fwd.h>
#include <Protos/EnumMacros.h>
#include <Protos/plan_node_utils.pb.h>

namespace DB
{
namespace Protos
{
    class Partitioning;
}
class Property;
using PropertySet = std::vector<Property>;
using PropertySets = std::vector<PropertySet>;
using SymbolEquivalences = Equivalences<String>;
using SymbolEquivalencesPtr = std::shared_ptr<SymbolEquivalences>;

class Constants;

using CTEId = UInt32;
class Context;

/**
 * A partition operation divides a relation into disjoint subsets, called partitions.
 * A partition function defines which rows belong to which partitions. Partitioning
 * applies to the whole relation.
 */
class Partitioning
{
public:
    ENUM_WITH_PROTO_CONVERTER(
        Handle, // enum name
        Protos::Partitioning::Handle, // proto enum message
        (SINGLE, 0),
        (COORDINATOR, 1),
        (FIXED_HASH, 2),
        (FIXED_ARBITRARY, 3),
        (FIXED_BROADCAST, 4),
        (SCALED_WRITER, 5),
        // Corresponding to this sharding key definition syntax: bucket(column_name, buckets),
        // only the BUCKET_TABLE type can use BUCKET_REPARTITION.
        (BUCKET_TABLE, 6),
        (ARBITRARY, 7),
        (FIXED_PASSTHROUGH, 8),
        (UNKNOWN, 9));

    ENUM_WITH_PROTO_CONVERTER(
        Component, // enum name
        Protos::Partitioning::Component, // proto enum message
        (ANY, 0),
        (COORDINATOR, 1),
        (WORKER, 2));


    Partitioning(const Names & columns_) : Partitioning(Handle::FIXED_HASH, columns_) { }

    Partitioning(
        enum Handle handle_ = Handle::UNKNOWN,
        Names columns_ = {},
        bool require_handle_ = false,
        UInt64 buckets_ = 0,
        ASTPtr bucket_expr_ = nullptr,
        bool enforce_round_robin_ = true,
        Component component_ = Component::ANY,
        bool exactly_match_ = false,
        bool satisfy_worker_ = false)
        : handle(handle_)
        , columns(std::move(columns_))
        , require_handle(require_handle_)
        , buckets(buckets_)
        , bucket_expr(bucket_expr_)
        , enforce_round_robin(enforce_round_robin_)
        , component(component_)
        , exactly_match(exactly_match_)
        , satisfy_worker(satisfy_worker_)
    {
    }
    void setHandle(Handle handle_) { handle = handle_; }
    enum Handle getHandle() const { return handle; }
    const Names & getColumns() const { return columns; }
    void setColumns(Names columns_)
    {
        columns = std::move(columns_);
    }
    UInt64 getBuckets() const { return buckets; }
    void setBuckets(UInt64 buckets_) { buckets = buckets_; }
    bool isEnforceRoundRobin() const { return enforce_round_robin; }
    void setEnforceRoundRobin(bool enforce_round_robin_) { enforce_round_robin = enforce_round_robin_; }
    bool isRequireHandle() const { return require_handle; }
    void setRequireHandle(bool require_handle_) { require_handle = require_handle_; }
    Component getComponent() const { return component; }
    void setComponent(Component component_) { component = component_; }
    bool isExactlyMatch() const { return exactly_match; }

    bool isPartitionHandle() const { return handle == Handle::BUCKET_TABLE || handle == Handle::FIXED_HASH; }

    bool isExchangeSchema(bool support_bucket_shuffle) const;
    bool isSimpleExchangeSchema(bool support_bucket_shuffle) const;

    ASTPtr getShuffleExpr() const;

    String getHashFunc(String default_func) const;
    Array getParams() const;

    void resetIfPartitionHandle()
    {
        if (!isPartitionHandle())
        {
            return;
        }
        this->columns = {};
        this->handle = Handle::UNKNOWN;
        this->bucket_expr = nullptr;
        this->buckets = 0;
    }

    bool isSatisfyWorker() const
    {
        return satisfy_worker;
    }

    void setSatisfyWorker(bool satisfy_worker_)
    {
        this->satisfy_worker = satisfy_worker_;
    }

    Partitioning translate(const std::unordered_map<String, String> & identities) const;
    Partitioning normalize(const SymbolEquivalences & symbol_equivalences) const;
    bool satisfy(const Partitioning &, const Constants & constants) const;
    bool isPartitionOn(const Partitioning &, const Constants & constants) const;

    bool isPreferred() const { return preferred; }
    void setPreferred(bool preferred_) { preferred = preferred_; }

    size_t hash() const;
    bool operator==(const Partitioning & other) const
    {
        return preferred == other.preferred && handle == other.handle && columns == other.columns && require_handle == other.require_handle && buckets == other.buckets
            && enforce_round_robin == other.enforce_round_robin && ASTEquality::compareTree(bucket_expr, other.bucket_expr);
    }

    ASTPtr getBucketExpr() const { return bucket_expr; }
    void setBucketExpr(const ASTPtr & bucket_expr_) { bucket_expr = bucket_expr_; }

    String toString() const;

    void toProto(Protos::Partitioning & proto) const;
    static Partitioning fromProto(const Protos::Partitioning & proto);

private:
    enum Handle handle;
    Names columns;
    bool require_handle;
    UInt64 buckets;
    ASTPtr bucket_expr;
    bool enforce_round_robin;
    Component component;
    bool exactly_match;
    bool satisfy_worker;
    bool preferred = false;
};

ENUM_WITH_PROTO_CONVERTER(
    SortOrder, // enum name
    Protos::SortOrder, // proto enum message
    (ASC_NULLS_FIRST),
    (ASC_NULLS_LAST),
    (ASC_ANY), // for not null type, nulls first/last is useless.
    (DESC_NULLS_FIRST),
    (DESC_NULLS_LAST),
    (DESC_ANY),
    (ANY),
    (UNKNOWN));

class SortColumn
{
public:
    static SortOrder directionToSortOrder(int direction, int nulls_direction)
    {
        if (direction == 1)
        {
            if (nulls_direction == 1)
                return SortOrder::ASC_NULLS_LAST;
            else if (nulls_direction == -1)
                return SortOrder::ASC_NULLS_FIRST;
            else if (nulls_direction == 0)
                return SortOrder::ASC_ANY;
        }
        else if (direction == -1)
        {
            if (nulls_direction == 1)
                return SortOrder::DESC_NULLS_LAST;
            else if (nulls_direction == -1)
                return SortOrder::DESC_NULLS_FIRST;
            else if (nulls_direction == 0)
                return SortOrder::DESC_ANY;
        }
        else if (direction == 0 && nulls_direction == 0)
            return SortOrder::ANY;
        return SortOrder::UNKNOWN;
    }
    static SortOrder toReverseOrder(SortOrder sort_order);

    SortColumn(String name_, SortOrder order_) : name(std::move(name_)), order(order_) { }
    explicit SortColumn(const SortColumnDescription & sort_column_description) : name(sort_column_description.column_name)
    {
        order = directionToSortOrder(sort_column_description.direction, sort_column_description.nulls_direction);
    }

    const String & getName() const { return name; }
    SortOrder getOrder() const { return order; }
    SortColumn toReverseOrder() const { return SortColumn{name, toReverseOrder(order)}; }

    SortColumnDescription toSortColumnDesc() const
    {
        int direction;
        int nulls_direction;
        switch (order)
        {
            case SortOrder::ASC_NULLS_FIRST: {
                direction = 1;
                nulls_direction = -1;
                break;
            }
            case SortOrder::ASC_NULLS_LAST: {
                direction = 1;
                nulls_direction = 1;
                break;
            }
            case SortOrder::ASC_ANY: {
                direction = 1;
                nulls_direction = 0;
                break;
            }
            case SortOrder::DESC_NULLS_FIRST: {
                direction = -1;
                nulls_direction = -1;
                break;
            }
            case SortOrder::DESC_NULLS_LAST: {
                direction = -1;
                nulls_direction = 1;
                break;
            }
            case SortOrder::DESC_ANY: {
                direction = -1;
                nulls_direction = 0;
                break;
            }
            case SortOrder::ANY: {
                direction = 0;
                nulls_direction = 0;
                break;
            }
            case SortOrder::UNKNOWN: {
                direction = 2;
                nulls_direction = 2;
                break;
            }
        }
        return SortColumnDescription{name, direction, nulls_direction};
    }

    bool operator==(const SortColumn & other) const { return name == other.name && order == other.order; }
    size_t hash() const;
    String toString() const;

private:
    String name;
    SortOrder order;
};

class Sorting : public std::vector<SortColumn>
{
public:
    Sorting() = default;
    explicit Sorting(const SortDescription & sort_description)
    {
        for (const auto & item : sort_description)
            emplace_back(SortColumn(item));
    }

    Sorting translate(const std::unordered_map<String, String> & identities) const;
    Sorting normalize(const SymbolEquivalences & symbol_equivalences) const;
    SortDescription toSortDesc() const
    {
        SortDescription res;
        for (const auto & item : *this)
        {
            res.emplace_back(item.toSortColumnDesc());
        }
        return res;
    }

    Sorting toReverseOrder() const;

    size_t hash() const;
    String toString() const;
};

class Grouping
{
public:
    explicit Grouping(Names columns_) : columns(std::move(columns_)) { }
    Names getColumns() { return columns; }

private:
    Names columns;
};

class CTEDescription
{
public:
    CTEDescription() : CTEDescription(false, Partitioning::Handle::ARBITRARY)
    {
    }

    static CTEDescription inlined();
    static CTEDescription arbitrary()
    {
        return CTEDescription{};
    }
    static CTEDescription from(const Property &);

    bool operator==(const CTEDescription & other) const;
    size_t hash() const;
    String toString() const;

    CTEDescription translate(const std::unordered_map<String, String> & identities) const;
    const Partitioning & getNodePartitioning() const { return node_partitioning; }
    Partitioning & getNodePartitioningRef() { return node_partitioning; }
    bool isInlined() const { return is_inlined; }
    bool isShared() const { return !is_inlined; }
    bool isArbitrary() const { return !is_inlined && node_partitioning.getHandle() == Partitioning::Handle::ARBITRARY; }

    static Property createCTEDefGlobalProperty(const Property & property, CTEId cte_id);
    static Property
    createCTEDefLocalProperty(const Property & property, CTEId cte_id, const std::unordered_map<String, String> & identities_mapping);

private:
    explicit CTEDescription(bool is_inlined_, Partitioning node_partitioning_)
        : is_inlined(is_inlined_), node_partitioning(std::move(node_partitioning_))
    {
    }

    // cte is inlined.
    bool is_inlined;
    // Description of the partitioning of the data across nodes
    Partitioning node_partitioning;
};

class CTEDescriptions : public std::map<CTEId, CTEDescription>
{
public:
    size_t hash() const;
    CTEDescriptions translate(const std::unordered_map<String, String> & identities) const;
    String toString() const;

    void filter(const std::unordered_set<CTEId> & allowed);
    };

struct WorkloadTablePartitioning
{
    std::optional<QualifiedColumnName> partition_key;

    static WorkloadTablePartitioning starPartition() { return WorkloadTablePartitioning{std::nullopt}; }

    bool isStarPartitioned() const { return !partition_key.has_value(); }

    QualifiedColumnName getPartitionKey() const { return partition_key.value(); }

    bool operator==(const WorkloadTablePartitioning & other) const { return partition_key == other.partition_key; }
};

class TableLayout : public std::map<QualifiedTableName, WorkloadTablePartitioning>
{
public:
    using std::map<QualifiedTableName, WorkloadTablePartitioning>::map;
    size_t hash() const;
    String toString() const;
};

class Property
{
public:
    explicit Property(
        Partitioning node_partitioning_ = Partitioning(Partitioning::Handle::ARBITRARY),
        Partitioning stream_partitioning_ = Partitioning(Partitioning::Handle::ARBITRARY),
        Sorting sorting_ = {})
        : node_partitioning(std::move(node_partitioning_))
        , stream_partitioning(std::move(stream_partitioning_))
        , sorting(std::move(sorting_))
    {
    }

    const Partitioning & getNodePartitioning() const { return node_partitioning; }
    Partitioning & getNodePartitioningRef() { return node_partitioning; }
    const Partitioning & getStreamPartitioning() const { return stream_partitioning; }
    Partitioning & getStreamPartitioningRef() { return stream_partitioning; }
    const Sorting & getSorting() const { return sorting; }
    const CTEDescriptions & getCTEDescriptions() const { return cte_descriptions; }
    CTEDescriptions & getCTEDescriptions() { return cte_descriptions; }
    const TableLayout & getTableLayout() const { return table_layout; }

    void setPreferred(bool preferred_)
    {
        node_partitioning.setPreferred(preferred_);
        stream_partitioning.setPreferred(preferred_);
    }

    void setNodePartitioning(Partitioning node_partitioning_) { node_partitioning = std::move(node_partitioning_); }
    Property withNodePartitioning(Partitioning node_partitioning_) const
    {
        Property result = *this;
        result.setNodePartitioning(node_partitioning_);
        return result;
    }
    void setStreamPartitioning(Partitioning stream_partitioning_) { stream_partitioning = std::move(stream_partitioning_); }
    Property withStreamPartitioning(Partitioning stream_partitioning_) const
    {
        Property result = *this;
        result.setStreamPartitioning(stream_partitioning_);
        return result;
    }
    void setCTEDescriptions(CTEDescriptions descriptions) { cte_descriptions = std::move(descriptions); }
    void setSorting(Sorting sorting_) { sorting = std::move(sorting_); }
    void setTableLayout(TableLayout table_layout_) { table_layout = std::move(table_layout_); }

    Property clearSorting() const
    {
        auto result = Property{node_partitioning, stream_partitioning, {}};
        result.setCTEDescriptions(cte_descriptions);
        return result;
    }

    Property translate(const std::unordered_map<String, String> & identities) const;
    Property normalize(const SymbolEquivalences & symbol_equivalences) const;

    bool operator==(const Property & other) const
    {
        return node_partitioning == other.node_partitioning
            && stream_partitioning == other.stream_partitioning && sorting == other.sorting && cte_descriptions == other.cte_descriptions
            && table_layout == other.table_layout;
    }

    bool operator!=(const Property & other) const { return !(*this == other); }

    size_t hash() const;
    String toString() const;

private:
    
    // Description of the partitioning of the data across nodes
    Partitioning node_partitioning;
    // Description of the partitioning of the data across streams
    Partitioning stream_partitioning;
    // Description of the sort order of the columns
    Sorting sorting;
    // Description of the group property of the columns
    // Grouping grouping;
    // Description of the requirements of the common table expressions.
    CTEDescriptions cte_descriptions;
    // Description of the what-if table layout
    TableLayout table_layout;
};

/**
 * Defines struct for hashing a property set
 */
struct PropertyHash
{
    /**
     * Hashes a Property
     * @param property Property to hash
     * @returns hash code
     */
    std::size_t operator()(const Property & property) const { return property.hash(); }
};

/**
 * Defines struct for hashing CTEDescription
 */
struct CTEDescriptionHash
{
    /**
     * Hashes a CTEDescription
     * @param cte_description CTEDescription to hash
     * @returns hash code
     */
    std::size_t operator()(const CTEDescription & cte_description) const { return cte_description.hash(); }
};

struct TableLayoutHash
{
    size_t operator()(const TableLayout & layout) const { return layout.hash(); }
};

struct PlanAndProp
{
    PlanNodePtr plan;
    Property property;
};

struct PlanPropEquivalences
{
    PlanNodePtr plan;
    Property property;
    SymbolEquivalencesPtr equivalences;
};

}
