
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

#include <Core/Types.h>
#include <Storages/IndexFile/Options.h>
#include <Storages/IndexFile/Status.h>
#include <Common/Slice.h>

#include <memory>

namespace DB::IndexFile
{
struct IndexFileInfo
{
    using uint128 = std::pair<uint64_t, uint64_t>;
    String file_path;
    String smallest_key;        /// smallest user key in file
    String largest_key;         /// largest user key in file
    uint64_t file_size = 0;     /// file size in bytes
    uint128 file_hash{0, 0};    /// file hash
    uint64_t num_entries = 0;   /// number of entries in file
};

class IndexFileWriter
{
public:
    explicit IndexFileWriter(const Options & options);
    ~IndexFileWriter();

    /// Prepare IndexFileWriter to write into file located at "file_path".
    Status Open(const String & file_path);

    /// Add a key with value to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    Status Add(const Slice & key, const Slice & value);

    /// Finalize writing to index file and close file.
    /// An optional IndexFileInfo pointer can be passed to the function
    /// which will be populated with information about the created sst file.
    Status Finish(IndexFileInfo * file_info = nullptr);

private:
    struct Rep;
    std::unique_ptr<Rep> rep;
};

}
