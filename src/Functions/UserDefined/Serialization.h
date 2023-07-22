#ifndef SERIALIZATION_H_NK36AUGR
#define SERIALIZATION_H_NK36AUGR
#include <optional>
#include <Common/PODArray.h>
#include <Core/ColumnWithTypeAndName.h>
#include "Proto.h"

namespace DB {

struct metastore {
    std::vector<struct UDFMeta> metas;
    std::vector<char> extras;
    std::vector<std::function<void(bool success)>> output;
    bool isOut;
};

struct metastoreex {
    struct metastore ms;
    std::vector<ColumnPtr> ptrs;
    uint64_t prefix;
    uint16_t idx;
};

void serializeColumn(const ColumnWithTypeAndName &col,
                     struct metastoreex *mse);

}

#endif /* end of include guard: SERIALIZATION_H_NK36AUGR */
