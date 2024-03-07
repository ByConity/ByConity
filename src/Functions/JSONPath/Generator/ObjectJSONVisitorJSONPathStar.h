#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathStar.h>
#include <Functions/JSONPath/Generator/IObjectJSONVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
class ObjectJSONVisitorJSONPathStar : public IObjectJSONVisitor
{
public:
    explicit ObjectJSONVisitorJSONPathStar(ASTPtr)
    {
        current_index = 0;
    }

    const char * getName() const override { return "ObjectJSONVisitorJSONPathStar"; }

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
            this->setExhausted(true);
        }

        return status;
    }

    void reinitialize() override
    {
        current_index = 0;
        this->setExhausted(false);
    }

    void updateState() override
    {
        current_index++;
    }

private:
    UInt32 current_index;
};

}
