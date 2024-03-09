#pragma once

#include <Functions/JSONPath/Generator/VisitorStatus.h>
#include <Functions/FunctionsJSON.h>

namespace DB
{
class IObjectJSONVisitor
{
public:
    virtual const char * getName() const = 0;

    /**
     * Applies this visitor to document and mutates its state
     * @param element simdjson element
     */
    virtual VisitorStatus visit(ObjectIterator & iterator) = 0;

    /**
     * Applies this visitor to document, but does not mutate state
     * @param element simdjson element
     */
    virtual VisitorStatus apply(ObjectIterator & iterator) = 0;

    /**
     * Restores visitor's initial state for later use
     */
    virtual void reinitialize() = 0;

    virtual void updateState() = 0;

    bool isExhausted() { return is_exhausted; }

    void setExhausted(bool exhausted) { is_exhausted = exhausted; }

    virtual ~IObjectJSONVisitor() = default;

private:
    /**
     * This variable is for detecting whether a visitor's next visit will be able
     *  to yield a new item.
     */
    bool is_exhausted = false;
};

}
