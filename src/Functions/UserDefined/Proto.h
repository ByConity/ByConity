#ifndef PROTO_H_3YRSG6FM
#define PROTO_H_3YRSG6FM
#include <cstdint>
#include <cstdio>
#include <vector>
#include <Common/BitHelpers.h>

namespace DB {

enum UDFMetaFlags {
    HAS_NESTED_DATA,
};

enum Method {
    SCALAR_CALL = BIT(0),
    ADD_BATCH = BIT(1),
    ADD_BATCH_SINGLE_PLACE = BIT(2),
    MERGE_BATCH = BIT(3),
    MERGE_DATA_BATCH = BIT(4),
    METHODS_CNT = 5
};

struct UDFMeta {
    uint8_t type;         /* column type, refer to DB::TypeIndex */
    uint8_t fd_cnt;       /* FD counts for the column, not including nested column */
    uint8_t flags;        /* bitmap of enum UDFMetaFlags */
    /* extra info cannot be stored in FD, e.g. N of FixedString, such info
     * is required to restore the column at the remote side */
    uint8_t extrasize;

    /* push extra info in byte array, each type itself need to reinterpret
     * the byte array according to the mutual contract */
    template<class T>
    void pushExtra(std::vector<char> &extras, const T &val)
    {
        const char *p = reinterpret_cast<const char *>(&val);
        this->extrasize = sizeof(T);
        for (int i = 0; i < this->extrasize; i++)
        {
            extras.push_back(p[i]);
        }
    }
};

}

#endif /* end of include guard: PROTO_H_3YRSG6FM */
