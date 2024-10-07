#pragma once

#include <Core/SortDescription.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPlan/TopNModel.h>
#include <Common/SipHash.h>

#include <queue>
#include <unordered_set>

namespace DB
{

namespace TopNFilteringImpl
{
    struct Entry
    {
        const ColumnRawPtrs * columns;
        size_t pos;
    };

    struct EntryHash
    {
        std::size_t operator()(const Entry & e) const
        {
            SipHash hash;
            for (const auto & c : *e.columns)
                c->updateHashWithValue(e.pos, hash);
            return static_cast<std::size_t>(hash.get64());
        }
    };

    struct EntryEquals
    {
        bool operator()(const Entry & lhs, const Entry & rhs) const
        {
            auto col_size = lhs.columns->size();
            assert(rhs.columns->size() == col_size);

            for (size_t i = 0; i < col_size; ++i)
                if (lhs.columns->at(i)->compareAt(lhs.pos, rhs.pos, *rhs.columns->at(i), 1) != 0)
                    return false;

            return true;
        }
    };

    using Directions = std::vector<int>;

    struct EntryCompare
    {
        Directions directions;
        Directions nulls_directions;
        bool contains_equality;

        bool operator()(const Entry & lhs, const Entry & rhs) const
        {
            auto col_size = lhs.columns->size();
            assert(rhs.columns->size() == col_size);
            assert(directions.size() == col_size);
            assert(nulls_directions.size() == col_size);

            int result = 0;
            for (size_t i = 0; i < col_size; ++i)
            {
                result = directions.at(i) * lhs.columns->at(i)->compareAt(lhs.pos, rhs.pos, *rhs.columns->at(i), nulls_directions.at(i));

                if (result < 0)
                    return true;

                if (result > 0)
                    return false;
            }
            return contains_equality;
        }
    };
};

class TopNFilteringBaseTransform : public ISimpleTransform
{
public:
    TopNFilteringBaseTransform(const Block & header_, SortDescription sort_description_, UInt64 size_, TopNModel model_);

protected:
    SortDescription sort_description;
    UInt64 size;
    TopNModel model;
};

class TopNFilteringByLimitingTransform : public TopNFilteringBaseTransform
{
public:
    using TopNFilteringBaseTransform::TopNFilteringBaseTransform;
    String getName() const override
    {
        return "TopNFilteringByLimitingTransform";
    }

protected:
    void transform(Chunk & chunk) override;
};

class TopNFilteringByHeapTransform : public TopNFilteringBaseTransform
{
public:
    TopNFilteringByHeapTransform(const Block & header_, SortDescription sort_description_, UInt64 size_, TopNModel model_);
    String getName() const override
    {
        return "TopNFilteringByHeapTransform";
    }

protected:
    void transform(Chunk & chunk) override;

public:
    struct FilterResult
    {
        bool kept_in_output;
        bool added_to_state;
    };

    using Entry = TopNFilteringImpl::Entry;
    using EntryHash = TopNFilteringImpl::EntryHash;
    using EntryEquals = TopNFilteringImpl::EntryEquals;
    using EntryCompare = TopNFilteringImpl::EntryCompare;
    using Directions = TopNFilteringImpl::Directions;

    class State
    {
    public:
        virtual ~State() = default;

        // Check if a value can be filtered by topn and if it should be added to the heap
        virtual FilterResult filter(const Entry &) = 0;

        // Insert a value into the heap. Why we use a seperate interface? Because before adding
        // the value to the heap, we copy it to the internal column, to avoid holding too many columns.
        virtual void add(const Entry &) = 0;
    };

    // For 'DENSE_RANK', we keep at most top n distinct values in the heap. A value passes
    // the filter if the heap not reach the capacity or the value is less than or equal to
    // the heap top. A value is inserted into the heap if it passes the filter and there is
    // no same value in the heap.
    class DenseRankState : public State
    {
    public:
        DenseRankState(const Directions & sort_directions, const Directions & sort_nulls_directions, int capacity_)
            : heap(EntryCompare{
                .directions = sort_directions,
                .nulls_directions = sort_nulls_directions,
                .contains_equality = false}) // std::priority_queue requires a strict weak order
            , compartor{.directions = sort_directions, .nulls_directions = sort_nulls_directions, .contains_equality = true}
            , capacity(capacity_)
        {
        }

        FilterResult filter(const Entry & e) override
        {
            if (heap.size() < capacity || compartor(e, heap.top()))
                return {.kept_in_output = true, .added_to_state = !set.count(e)};

            return {.kept_in_output = false, .added_to_state = false};
        }

        void add(const Entry & e) override
        {
            assert(!set.count(e));
            assert(heap.size() <= capacity);
            set.insert(e);
            if (heap.size() == capacity)
                heap.pop();
            heap.push(e);
        }

    private:
        std::unordered_set<Entry, EntryHash, EntryEquals> set;
        std::priority_queue<Entry, std::vector<Entry>, EntryCompare> heap;
        EntryCompare compartor;
        const size_t capacity;
    };

    // For 'ROW_NUMBER', we keep at most top n values in the heap. A value passes the filter
    // if the heap not reach the capacity or the value is less than the heap top. A value is
    // inserted into the heap if it passes the filter.
    class RowNumberState : public State
    {
    public:
        RowNumberState(const Directions & sort_directions, const Directions & sort_nulls_directions, int capacity_)
            : heap(EntryCompare{
                .directions = sort_directions,
                .nulls_directions = sort_nulls_directions,
                .contains_equality = false}) // std::priority_queue requires a strict weak order
            , compartor{.directions = sort_directions, .nulls_directions = sort_nulls_directions, .contains_equality = false}
            , capacity(capacity_)
        {
        }

        FilterResult filter(const Entry & e) override
        {
            if (heap.size() < capacity || compartor(e, heap.top()))
                return {.kept_in_output = true, .added_to_state = true};

            return {.kept_in_output = false, .added_to_state = false};
        }

        void add(const Entry & e) override
        {
            assert(heap.size() <= capacity);
            if (heap.size() == capacity)
                heap.pop();
            heap.push(e);
        }

    private:
        std::priority_queue<Entry, std::vector<Entry>, EntryCompare> heap;
        EntryCompare compartor;
        const size_t capacity;
    };

    using StatePtr = std::unique_ptr<State>;

private:
    MutableColumns cached_columns;
    ColumnRawPtrs raw_cached_columns;
    StatePtr state;
};
}
