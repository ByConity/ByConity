
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

#include <Storages/IndexFile/IndexFileReader.h>

#include <Storages/IndexFile/Env.h>
#include <Storages/IndexFile/Table.h>

namespace DB::IndexFile
{
struct IndexFileReader::Rep
{
    Rep(const Options & options_) : options(options_) { }

    Options options;
    std::unique_ptr<Table> table_reader;
};

IndexFileReader::IndexFileReader(const Options & options) : rep(new Rep(options))
{
}

IndexFileReader::~IndexFileReader() = default;

Status IndexFileReader::Open(const String & file_path)
{
    Status s;
    UInt64 file_size = 0;
    std::unique_ptr<RandomAccessFile> file;
    s = rep->options.env->GetFileSize(file_path, &file_size);
    if (s.ok())
        s = rep->options.env->NewRandomAccessFile(file_path, &file);
    if (s.ok())
        s = Table::Open(rep->options, std::move(file), file_size, &rep->table_reader);
    return s;
}

Status IndexFileReader::Open(const RemoteFileInfo & remote_file)
{
    std::unique_ptr<RandomAccessFile> file;
    Status s = rep->options.env->NewRandomAccessRemoteFileWithCache(remote_file, rep->options.remote_file_cache, &file);
    if (s.ok())
        s = Table::Open(rep->options, std::move(file), remote_file.size, &rep->table_reader);
    return s;
}

Status IndexFileReader::Get(const ReadOptions & options, const Slice & key, String * value)
{
    if (!rep->table_reader)
        return Status::InvalidArgument("File is not opened");
    return rep->table_reader->Get(options, key, value);
}

Status IndexFileReader::NewIterator(const ReadOptions & options, std::unique_ptr<Iterator> * out)
{
    if (!rep->table_reader)
        return Status::InvalidArgument("File is not opened");
    (*out).reset(rep->table_reader->NewIterator(options));
    return Status::OK();
}

size_t IndexFileReader::ResidentMemoryUsage() const
{
    size_t res = sizeof(Rep);
    if (rep->table_reader)
        res += rep->table_reader->ResidentMemoryUsage();
    return res;
}

}
