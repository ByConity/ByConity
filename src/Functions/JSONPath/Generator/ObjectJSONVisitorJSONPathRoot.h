#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathRoot.h>
#include <Functions/JSONPath/Generator/IObjectJSONVisitor.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

namespace DB
{
class ObjectJSONVisitorJSONPathRoot : public IObjectJSONVisitor
{
public:
    explicit ObjectJSONVisitorJSONPathRoot(ASTPtr) { }

    const char * getName() const override { return "ObjectJSONVisitorJSONPathRoot"; }

    VisitorStatus apply(ObjectIterator & /*iterator*/) override
    {
        /// No-op on document, since we are already passed document's root
        return VisitorStatus::Ok;
    }

    VisitorStatus visit(ObjectIterator & iterator) override
    {
        apply(iterator);
        this->setExhausted(true);
        return VisitorStatus::Ok;
    }

    void reinitialize() override { this->setExhausted(false); }

    void updateState() override { }
};

}
