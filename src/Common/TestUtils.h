#pragma once

#include <stdlib.h>

namespace DB
{

inline bool isCIEnv()
{
    auto * is_ci_env = getenv("IS_CI_ENV");
    return is_ci_env != nullptr;
}

}

