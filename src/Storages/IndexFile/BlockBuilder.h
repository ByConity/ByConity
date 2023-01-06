
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

#pragma once

#include <vector>

#include <stdint.h>
#include <Common/Slice.h>

namespace DB::IndexFile
{
struct Options;

class BlockBuilder
{
public:
    explicit BlockBuilder(const Options * options);

    // Reset the contents as if the BlockBuilder was just constructed.
    void Reset();

    // REQUIRES: Finish() has not been called since the last call to Reset().
    // REQUIRES: key is larger than any previously added key
    void Add(const Slice & key, const Slice & value);

    // Finish building the block and return a slice that refers to the
    // block contents.  The returned slice will remain valid for the
    // lifetime of this builder or until Reset() is called.
    Slice Finish();

    // Returns an estimate of the current (uncompressed) size of the block
    // we are building.
    size_t CurrentSizeEstimate() const;

    // Return true iff no entries have been added since the last Reset()
    bool empty() const { return buffer_.empty(); }

private:
    const Options * options_;
    std::string buffer_; // Destination buffer
    std::vector<uint32_t> restarts_; // Restart points
    int counter_; // Number of entries emitted since restart
    bool finished_; // Has Finish() been called?
    std::string last_key_;

    // No copying allowed
    BlockBuilder(const BlockBuilder &);
    void operator=(const BlockBuilder &);
};

}
