#pragma once
#include <cstdint>
#include "UDFIColumn.h"

class UDFColumnNullable final : public UDFIColumn
{
public:

    UDFColumnNullable(DB::TypeIndex type_idx_,
                      uint64_t prefix_,
                      uint32_t fd_,
                      UDFIColumn *nestedColumn_) :
        UDFIColumn(type_idx_, prefix_, fd_), nested(nestedColumn_) {
    }

    UDFIColumn *getNestedColumn() {
        return nested.get();
    }

private:
    std::unique_ptr<UDFIColumn> nested;
};
