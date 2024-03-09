#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generator/IObjectJSONVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
class ObjectJSONVisitorJSONPathMemberAccess : public IObjectJSONVisitor
{
public:
    explicit ObjectJSONVisitorJSONPathMemberAccess(ASTPtr member_access_ptr_)
        : member_access_ptr(member_access_ptr_->as<ASTJSONPathMemberAccess>()) { }

    const char * getName() const override { return "ObjectJSONVisitorJSONPathMemberAccess"; }

    VisitorStatus apply(ObjectIterator & iterator) override
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(iterator.getType().get());
        auto pos = type_tuple->tryGetPositionByName(member_access_ptr->member_name);
        const auto& type = type_tuple->getElement(*pos);
        auto subcolumn = assert_cast<const ColumnTuple &>(*iterator.getColumn()).getColumnPtr(*pos);
        iterator.setType(type);
        iterator.setColumn(subcolumn);
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(ObjectIterator & iterator) override
    {
        this->setExhausted(true);
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(iterator.getType().get());
        if (!type_tuple || !type_tuple->haveExplicitNames())
            return VisitorStatus::Error;

        auto pos = type_tuple->tryGetPositionByName(member_access_ptr->member_name);
        if (!pos)
            return VisitorStatus::Error;

        apply(iterator);
        return VisitorStatus::Ok;
    }

    void reinitialize() override { this->setExhausted(false); }

    void updateState() override { }

private:
    ASTJSONPathMemberAccess * member_access_ptr;
};

}
