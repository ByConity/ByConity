#pragma once

#include <Functions/JSONPath/Generator/IGenerator_fwd.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>
#include <Parsers/IAST.h>
#include <Functions/FunctionsJSON.h>

namespace DB
{

class IObjectJSONGenerator
{
public:
    IObjectJSONGenerator() = default;

    virtual const char * getName() const = 0;

    /**
     * Used to yield next non-ignored element describes by JSONPath query.
     *
     * @param element to be extracted into
     * @return true if generator is not exhausted
     */
    virtual VisitorStatus getNextItem(ObjectIterator & iterator) = 0;

    virtual ~IObjectJSONGenerator() = default;
};

}
