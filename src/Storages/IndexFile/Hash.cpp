
/*
 * Copyright (2022) ByteDance Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string.h>
#include <Storages/IndexFile/Hash.h>
#include <Common/Coding.h>

#ifndef FALLTHROUGH_INTENDED
#if defined(__clang__)
#define FALLTHROUGH_INTENDED [[clang::fallthrough]]
#elif defined(__GNUC__) && __GNUC__ >= 7
#define FALLTHROUGH_INTENDED [[gnu::fallthrough]]
#else
#define FALLTHROUGH_INTENDED do {} while (0)
#endif
#endif

namespace DB::IndexFile
{
uint32_t Hash(const char * data, size_t n, uint32_t seed)
{
    // Similar to murmur hash
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char * limit = data + n;
    uint32_t h = seed ^ (n * m);

    // Pick up four bytes at a time
    while (data + 4 <= limit)
    {
        uint32_t w = DecodeFixed32(data);
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - data)
    {
        case 3:
            h += static_cast<unsigned char>(data[2]) << 16;
            FALLTHROUGH_INTENDED;
        case 2:
            h += static_cast<unsigned char>(data[1]) << 8;
            FALLTHROUGH_INTENDED;
        case 1:
            h += static_cast<unsigned char>(data[0]);
            h *= m;
            h ^= (h >> r);
            break;
    }
    return h;
}

}
