
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

#include <stdio.h>
#include <Storages/IndexFile/Status.h>

namespace DB::IndexFile
{
const char * Status::CopyState(const char * state)
{
    uint32_t size;
    memcpy(&size, state, sizeof(size));
    char * result = new char[size + 5];
    memcpy(result, state, size + 5);
    return result;
}

Status::Status(Code code, const Slice & msg, const Slice & msg2)
{
    assert(code != kOk);
    const uint32_t len1 = msg.size();
    const uint32_t len2 = msg2.size();
    const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
    char * result = new char[size + 5];
    memcpy(result, &size, sizeof(size));
    result[4] = static_cast<char>(code);
    memcpy(result + 5, msg.data(), len1);
    if (len2)
    {
        result[5 + len1] = ':';
        result[6 + len1] = ' ';
        memcpy(result + 7 + len1, msg2.data(), len2);
    }
    state_ = result;
}

std::string Status::ToString() const
{
    if (state_ == nullptr)
    {
        return "OK";
    }
    else
    {
        // char tmp[30];
        const char * type;
        switch (code())
        {
            case kOk:
                type = "OK";
                break;
            case kNotFound:
                type = "NotFound: ";
                break;
            case kCorruption:
                type = "Corruption: ";
                break;
            case kNotSupported:
                type = "Not implemented: ";
                break;
            case kInvalidArgument:
                type = "Invalid argument: ";
                break;
            case kIOError:
                type = "IO error: ";
                break;
                // default:
                //     snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
                //     type = tmp;
                //     break;
        }
        std::string result(type);
        uint32_t length;
        memcpy(&length, state_, sizeof(length));
        result.append(state_ + 5, length);
        return result;
    }
}

}
