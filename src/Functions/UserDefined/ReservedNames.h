#pragma once
#include <array>

static inline bool isReservedName(const char *name)
{
    static const char * reserved_names[] = {
        "iudf.py",
        "iudaf.py",
        "overload.py",
    };

    for (const auto & curr : reserved_names) {
        if (strcmp(curr, name) == 0)
            return true;
    }

    return false;
}
