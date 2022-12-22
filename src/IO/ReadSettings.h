#pragma once

#include <cstddef>
#include <Core/Defines.h>
#include <IO/MMappedFileCache.h>

class MMappedFileCache;

namespace DB
{

struct ReadSettings
{
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t estimated_size = 0;
    size_t aio_threshold = 0;
    size_t mmap_threshold = 0;

    MMappedFileCache* mmap_cache = nullptr;
};

}
