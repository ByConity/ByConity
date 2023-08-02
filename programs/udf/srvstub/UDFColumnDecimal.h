#pragma once
#include "UDFIColumn.h"


/** A template for columns that use a simple array to store.
 */
template <typename T>
class UDFColumnDecimal:public UDFIColumn
{

public:

    UDFColumnDecimal() = default;

    UDFColumnDecimal(DB::TypeIndex type_idx_, uint64_t prefix_, uint32_t fd_, uint64_t scale_)
    : UDFIColumn(type_idx_, prefix_, fd_), scale(scale_){}

    uint64_t getScale()
    {
        return scale;
    }

private:

    uint64_t scale;

};
