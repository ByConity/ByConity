#pragma once

#include <stdlib.h>

namespace DB
{

inline bool isCIEnv()
{
    auto * ci_job_name = getenv("CI_JOB_NAME");
    return ci_job_name != nullptr;
}

}

