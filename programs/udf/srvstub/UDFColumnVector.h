#pragma once
#include "UDFIColumn.h"


/** A template for columns that use a simple array to store.
 */
template <typename T>
class UDFColumnVector : public UDFIColumn
{
public:
    UDFColumnVector() = default;

    UDFColumnVector(DB::TypeIndex type_idx_, uint64_t prefix_, uint32_t fd_)
    : UDFIColumn(type_idx_, prefix_, fd_){}

};
