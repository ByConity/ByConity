#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/Generator/IObjectJSONVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
class ObjectJSONVisitorJSONPathRange : public IObjectJSONVisitor
{
public:
    explicit ObjectJSONVisitorJSONPathRange(ASTPtr range_ptr_) : range_ptr(range_ptr_->as<ASTJSONPathRange>())
    {
        current_range = 0;
        current_index = range_ptr->ranges[current_range].first;
    }

    const char * getName() const override { return "ObjectJSONVisitorJSONPathRange"; }

    VisitorStatus apply(ObjectIterator & iterator) override
    {
        const auto * type_array = typeid_cast<const DataTypeArray *>(iterator.getType().get());

        const auto & column_array = assert_cast<const ColumnArray &>(*iterator.getColumn());
        const auto & offsets = column_array.getOffsets();
        const auto & row = iterator.getRow();

        const auto & type = type_array->getNestedType();
        const auto & column = column_array.getDataPtr();
        iterator.setType(type);
        iterator.setColumn(column);
        iterator.setRow(offsets[row - 1] + current_index);
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(ObjectIterator & iterator) override
    {
        const auto * type_array = typeid_cast<const DataTypeArray *>(iterator.getType().get());
        if (!type_array)
        {
            this->setExhausted(true);
            return VisitorStatus::Error;
        }

        const auto & column_array = assert_cast<const ColumnArray &>(*iterator.getColumn());
        const auto & offsets = column_array.getOffsets();

        const auto & row = iterator.getRow();
        UInt64 array_size = offsets[row] - offsets[row - 1];

        VisitorStatus status;
        if (current_index < array_size)
        {
            apply(iterator);
            status = VisitorStatus::Ok;
        }
        else
        {
            status = VisitorStatus::Ignore;
        }

        if (current_index + 1 == range_ptr->ranges[current_range].second
            && current_range + 1 == range_ptr->ranges.size())
        {
            this->setExhausted(true);
        }

        return status;
    }

    void reinitialize() override
    {
        current_range = 0;
        current_index = range_ptr->ranges[current_range].first;
        this->setExhausted(false);
    }

    void updateState() override
    {
        current_index++;
        if (current_index == range_ptr->ranges[current_range].second)
        {
            current_range++;
            current_index = range_ptr->ranges[current_range].first;
        }
    }

private:
    ASTJSONPathRange * range_ptr;
    size_t current_range;
    UInt32 current_index;
};

}
