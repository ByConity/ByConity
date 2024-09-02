#pragma once
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <boost/core/span.hpp>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>
#include <IO/VarInt.h>

#include "Common/Exception.h"
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <common/defines.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/SipHash.h>
#include <common/unaligned.h>


// Same limits as AggregateFunctionGroupArray
#define AGGREGATE_FUNCTION_ORDER_BY_MAX_ARRAY_SIZE 0xFFFFFF

namespace DB
{
struct Settings;
namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

struct SortColumnDataTypeWithDirection
{
    DataTypePtr column_data_type;
    IColumn::PermutationSortDirection sort_direction;
};

struct AggregateFunctionOrderByGenericData
{
    struct Header
    {
        UInt64 size; // size of serialized row, does not include sizeof(Header)
        UInt64 sort_columns_offset; // offset of serialized sorted columns, does not include sizeof(Header)
    };

    struct Node
    {
    private:
        char * header_ptr; // points to Header, which lies before start of row; set private to prevent accidental cast
    public:
        explicit Node(const char * header_ptr_) : header_ptr(const_cast<char *>(header_ptr_)) { }

        char * getStartOfRow() const { return header_ptr + sizeof(Header); }

        char * getSortColumns() const
        {
            Header header = getHeader();
            return header_ptr + sizeof(Header) + header.sort_columns_offset;
        }

        Header getHeader() const { return unalignedLoad<Header>(header_ptr); }

        void setHeader(const Header & header) { unalignedStore<Header>(header_ptr, header); }

        static Node read(ReadBuffer & buf, Arena * arena)
        {
            Header header;
            buf.readStrict(reinterpret_cast<char *>(&header), sizeof(header));
            char * header_ptr = arena->alloc(sizeof(Header) + header.size);
            buf.readStrict(header_ptr + sizeof(Header), header.size);
            Node node(header_ptr);
            node.setHeader(header);
            return node;
        }

        void write(WriteBuffer & buf) const
        {
            Header header = getHeader();
            buf.write(header_ptr, sizeof(Header) + header.size);
        }

        Node clone(Arena * arena) const
        {
            Header header = getHeader();
            const char * cloned_header_ptr = arena->insert(header_ptr, sizeof(Header) + header.size);
            return Node(cloned_header_ptr);
        }
    };

    using Allocator = MixedAlignedArenaAllocator<alignof(Node), 4096>;
    using NodeArray = PODArray<Node, 32, Allocator>;
    NodeArray nodes;
    const std::vector<SortColumnDataTypeWithDirection> sort_columns_description;

    // working area
    IColumn::Permutation permutation;

    explicit AggregateFunctionOrderByGenericData() // Empty constructor to allow compilation with IAggregateFunctionDataHelper::create
    {
        chassert(false);
    }

    explicit AggregateFunctionOrderByGenericData(std::vector<SortColumnDataTypeWithDirection> sort_columns_description_)
        : sort_columns_description(std::move(sort_columns_description_))
    {
    }

    void merge(const AggregateFunctionOrderByGenericData & rhs, Arena * arena)
    {
        nodes.reserve(nodes.size() + rhs.nodes.size(), arena);
        for (const Node & node : rhs.nodes)
            nodes.push_back(node.clone(arena), arena);
    }

    void getNodesPermutation(Arena * arena [[maybe_unused]])
    {
        permutation.resize(nodes.size());
        if (sort_columns_description.size() == 1)
        {
            const auto & sort_column_description = sort_columns_description[0];
            MutableColumnPtr sort_column = sort_column_description.column_data_type->createColumn();
            sort_column->reserve(nodes.size());

            for (const Node node : nodes)
            {
                const char * const offset = node.getSortColumns();
                (void)sort_column->deserializeAndInsertFromArena(offset);
            }

            sort_column->getPermutation(
                sort_column_description.sort_direction, IColumn::PermutationSortStability::Unstable, 0, -1, permutation);
        }
        else
        {
            std::vector<const char *> offsets(nodes.size());
            EqualRanges ranges{{static_cast<size_t>(0), nodes.size()}};
            std::iota(permutation.begin(), permutation.end(), 0);

            for (size_t i = 0; i < nodes.size(); ++i)
                offsets[i] = nodes[i].getSortColumns();

            for (const SortColumnDataTypeWithDirection & sort_column_description : sort_columns_description)
            {
                MutableColumnPtr sort_column = sort_column_description.column_data_type->createColumn();
                sort_column->reserve(nodes.size());
                for (const char *& offset : offsets)
                    offset = sort_column->deserializeAndInsertFromArena(offset);
                sort_column->updatePermutation(
                    sort_column_description.sort_direction, IColumn::PermutationSortStability::Unstable, 0, -1, permutation, ranges);
            }
        }
    }

    void add(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        // serializeValueIntoArena may cause reallocation into non-aligned memory
        // so we might as well start out allocating a non-aligned Header
        // Technically, alignment of Arena::MemoryChunk is 16, which is stronger than alignment of Header (8), so Header is always aligned
        // even if allocContinue, so we could use alignedAlloc here as well...
        const char * begin = arena->alloc(sizeof(Header));
        StringRef value(begin, sizeof(Header));
        std::optional<size_t> sort_columns_offset;

        for (size_t i = 0; i < columns_num; ++i)
        {
            if (i + sort_columns_description.size() == columns_num)
                sort_columns_offset.emplace(value.size - sizeof(Header));

            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        nodes.push_back(Node(const_cast<char *>(value.data)), arena);
        chassert(sort_columns_offset.has_value());
        nodes.back().setHeader(Header{.size = value.size, .sort_columns_offset = *sort_columns_offset});
    }

    MutableColumns getArguments(const DataTypes & argument_types, Arena * arena)
    {
        chassert(sort_columns_description.size() < argument_types.size());
        const boost::span arguments_types_without_sort_column(&argument_types[0], argument_types.size() - sort_columns_description.size());
        getNodesPermutation(arena);
        MutableColumns argument_columns(arguments_types_without_sort_column.size());

        for (size_t i = 0; i < arguments_types_without_sort_column.size(); ++i)
        {
            argument_columns[i] = arguments_types_without_sort_column[i]->createColumn();
            argument_columns[i]->reserve(nodes.size());
        }

        for (const size_t node_idx : permutation)
        {
            const Node & node = nodes[node_idx];
            const char * begin = node.getStartOfRow();
            for (auto & column : argument_columns)
                begin = column->deserializeAndInsertFromArena(begin);
        }

        return argument_columns;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(nodes.size(), buf);
        for (const Node node : nodes)
            node.write(buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        UInt64 num_nodes;
        readVarUInt(num_nodes, buf);
        if (unlikely(num_nodes > AGGREGATE_FUNCTION_ORDER_BY_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size");

        nodes.reserve(num_nodes, arena);
        for (UInt64 i = 0; i < num_nodes; ++i)
            nodes.push_back(Node::read(buf, arena), arena);
    }
};

/** Adaptor for aggregate functions.
  * Adding -OrderBy suffix to aggregate function
  * Parameters:
      - is_order_by_ascending...: variadic UInt8
        The is_order_by_ascending parameters should correspond to entries in order_by_column_expression
      - num_params: UInt32
        Number of is_order_by_ascending parameters
    Arguments:
      - order_by_column_expression...: 
        The variadic columns should correspond to entries in is_order_by_ascending
**/
class AggregateFunctionOrderBy : public IAggregateFunctionDataHelper<AggregateFunctionOrderByGenericData, AggregateFunctionOrderBy>
{
private:
    AggregateFunctionPtr nested_func;
    size_t arguments_num;
    size_t prefix_size;
    std::vector<SortColumnDataTypeWithDirection> sort_columns_description;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    bool needAdjustAlignment() const noexcept { return alignOfData() < nested_func->alignOfData(); }

public:
    AggregateFunctionOrderBy(
        AggregateFunctionPtr nested_func_, std::vector<bool> is_ascending, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionOrderBy>(arguments, params_)
        , nested_func(nested_func_)
        , arguments_num(arguments.size())
        , prefix_size([this] {
            size_t nested_size = nested_func->alignOfData();
            return (sizeof(Data) + nested_size - 1) / nested_size * nested_size;
        }())
        , sort_columns_description([&is_ascending, &arguments] {
            std::vector<SortColumnDataTypeWithDirection> tmp;
            const auto arguments_it = arguments.end() - is_ascending.size();

            for (size_t i = 0; i < is_ascending.size(); ++i)
            {
                tmp.push_back(SortColumnDataTypeWithDirection{
                    .column_data_type = arguments_it[i],
                    .sort_direction
                    = is_ascending[i] ? IColumn::PermutationSortDirection::Ascending : IColumn::PermutationSortDirection::Descending,
                });
            }

            return tmp;
        }())
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override { this->data(place).serialize(buf); }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    template <bool MergeResult>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        MutableColumns arguments = this->data(place).getArguments(this->argument_types, arena);
        ColumnRawPtrs arguments_raw(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "The number of arguments should exceed 0.");

        nested_func->addBatchSinglePlace(arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);

        if constexpr (MergeResult)
            nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
        else
            nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    size_t sizeOfData() const override { return prefix_size + nested_func->sizeOfData(); }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data(sort_columns_description);
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        std::destroy_at(&this->data(place));
        nested_func->destroy(getNestedPlace(place));
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        std::destroy_at(&this->data(place));
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override { return nested_func->getName() + "OrderBy"; }

    DataTypePtr getReturnType() const override { return nested_func->getReturnType(); }

    bool allocatesMemoryInArena() const override { return true; }

    bool isState() const override { return nested_func->isState(); }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
