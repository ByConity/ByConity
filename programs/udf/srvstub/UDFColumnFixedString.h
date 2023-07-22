#pragma once

#include <cstdint>
#include <Core/Types.h>

#include "UDFIColumn.h"

class UDFColumnFixedString final: public UDFIColumn
{

public:

  UDFColumnFixedString(DB::TypeIndex type_idx_, uint64_t prefix_, uint32_t chars_fd_, uint64_t string_length_)
    : UDFIColumn(type_idx_, prefix_, chars_fd_), string_length(string_length_){}

    uint64_t getStringLength() const
    {
        return string_length;
    }

private:

    uint64_t string_length;

};
