#include <Processors/Transforms/TopNFilteringTransform.h>

#include <Common/PODArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

TopNFilteringBaseTransform::TopNFilteringBaseTransform(
    const Block & header_, SortDescription sort_description_, UInt64 size_, TopNModel model_)
    : ISimpleTransform(header_, header_, true), sort_description(std::move(sort_description_)), size(size_), model(model_)
{
    if (sort_description.empty())
        throw Exception("TopN sort description is empty", ErrorCodes::LOGICAL_ERROR);

    if (!size)
        throw Exception("TopN size must be greater than 0", ErrorCodes::LOGICAL_ERROR);

    if (model != TopNModel::DENSE_RANK && model != TopNModel::ROW_NUMBER)
        throw Exception("only support DENSE_RANK", ErrorCodes::NOT_IMPLEMENTED);
}

void TopNFilteringByLimitingTransform::transform(Chunk & chunk)
{
    using TopNFilteringImpl::Entry;
    using TopNFilteringImpl::EntryEquals;

    size_t num_rows_before_filtration = chunk.getNumRows();

    if (num_rows_before_filtration <= size)
        return;

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (model == TopNModel::ROW_NUMBER)
    {
        for (auto & col : block)
            col.column = col.column->cut(0, size);
        chunk.setColumns(block.getColumns(), size);
    }
    else // DENSE_RANK
    {
        ColumnRawPtrs sort_columns;
        for (const auto & sort_desc : sort_description)
            sort_columns.push_back(block.getByName(sort_desc.column_name).column.get());

        EntryEquals equals_op;

        size_t index = 0, count = 1;
        for (size_t i = 1; i < num_rows_before_filtration; i++)
        {
            if (!equals_op(Entry{&sort_columns, index}, Entry{&sort_columns, i}))
            {
                index = i;
                count++;
            }

            if (count > size)
            {
                for (auto & col : block)
                    col.column = col.column->cut(0, i);
                chunk.setColumns(block.getColumns(), i);
                return;
            }
        }

        chunk.setColumns(block.getColumns(), num_rows_before_filtration);
    }
}

TopNFilteringByHeapTransform::TopNFilteringByHeapTransform(
    const Block & header_, SortDescription sort_description_, UInt64 size_, TopNModel model_)
    : TopNFilteringBaseTransform(header_, std::move(sort_description_), size_, model_)
{
    using TopNFilteringImpl::Directions;

    const auto & header = getInputPort().getHeader();
    Directions sort_directions;
    Directions sort_nulls_directions;

    for (const auto & sort_desc : sort_description)
    {
        sort_directions.push_back(sort_desc.direction);
        sort_nulls_directions.push_back(sort_desc.nulls_direction);
        cached_columns.push_back(header.getByName(sort_desc.column_name).column->cloneEmpty());
        cached_columns.back()->reserve(size);
        raw_cached_columns.push_back(cached_columns.back().get());
    }

    switch (model)
    {
        case TopNModel::DENSE_RANK:
            state = std::make_unique<DenseRankState>(sort_directions, sort_nulls_directions, size);
            break;
        case TopNModel::ROW_NUMBER:
            state = std::make_unique<RowNumberState>(sort_directions, sort_nulls_directions, size);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unknown topn model");
    }
}

void TopNFilteringByHeapTransform::transform(Chunk & chunk)
{
    size_t num_rows_input = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    ColumnRawPtrs sort_columns;
    for (const auto & sort_desc : sort_description)
        sort_columns.push_back(block.getByName(sort_desc.column_name).column.get());

    IColumn::Filter filter_col;
    filter_col.reserve(num_rows_input);
    ssize_t num_rows_output = 0;

    for (size_t i = 0; i < num_rows_input; ++i)
    {
        auto result = state->filter(Entry{&sort_columns, i});

        if (result.kept_in_output)
        {
            filter_col.push_back(1U);
            ++num_rows_output;
        }
        else
        {
            filter_col.push_back(0U);
        }

        if (result.added_to_state)
        {
            for (size_t k = 0; k < sort_columns.size(); ++k)
                cached_columns.at(k)->insertFrom(*sort_columns.at(k), i);

            state->add(Entry{&raw_cached_columns, cached_columns.front()->size() - 1});
        }
    }

    for (auto & col : block)
        col.column = col.column->filter(filter_col, num_rows_output);

    chunk.setColumns(block.getColumns(), num_rows_output);
}
}
