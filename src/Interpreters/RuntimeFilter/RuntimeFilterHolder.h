#pragma once

#include <common/types.h>

namespace DB
{
using RuntimeFilterId = size_t;

class RuntimeFilterHolder
{
public:
    RuntimeFilterHolder(const String & query_id_, size_t segment_id_, RuntimeFilterId filter_id_)
        : query_id(query_id_), segment_id(segment_id_), filter_id(filter_id_)
    {
    }

    RuntimeFilterHolder(const RuntimeFilterHolder & other) = delete;
    RuntimeFilterHolder(RuntimeFilterHolder && other) = default;
    RuntimeFilterHolder & operator=(const RuntimeFilterHolder & other) = delete;
    RuntimeFilterHolder & operator=(RuntimeFilterHolder && other) = default;

    ~RuntimeFilterHolder();

private:
    String query_id;
    size_t segment_id;
    RuntimeFilterId filter_id;
};
}

