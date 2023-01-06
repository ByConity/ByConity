
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

#include <Storages/IndexFile/IndexFileWriter.h>

#include <Storages/IndexFile/Comparator.h>
#include <Storages/IndexFile/Env.h>
#include <Storages/IndexFile/TableBuilder.h>

namespace DB::IndexFile
{
struct IndexFileWriter::Rep
{
    Rep(const Options & options_) : options(options_) { }

    Options options;
    IndexFileInfo file_info;
    std::unique_ptr<WritableFile> file_writer;
    std::unique_ptr<TableBuilder> builder;
};

IndexFileWriter::IndexFileWriter(const Options & options) : rep(new Rep(options))
{
}

IndexFileWriter::~IndexFileWriter()
{
    if (rep->builder)
    {
        /// User did not call Finish() or Finish() failed, we need to
        /// abandon the builder.
        rep->builder->Abandon();
    }
}

Status IndexFileWriter::Open(const String & file_path)
{
    Status s;
    std::unique_ptr<WritableFile> file_writer;
    s = rep->options.env->NewWritableFile(file_path, &file_writer);
    if (!s.ok())
        return s;
    /// this writer may be used to create several files
    /// therefore we need to reset file info here
    rep->file_info = IndexFileInfo();
    rep->file_info.file_path = file_path;
    rep->file_writer = std::move(file_writer);
    rep->builder = std::make_unique<TableBuilder>(rep->options, rep->file_writer.get());
    return s;
}

Status IndexFileWriter::Add(const Slice & key, const Slice & value)
{
    Rep * r = rep.get();
    if (!r->builder)
        return Status::InvalidArgument("File is not opened");
    if (r->file_info.num_entries == 0)
    {
        r->file_info.smallest_key.assign(key.data(), key.size());
    }
    else if (r->options.comparator->Compare(key, r->file_info.largest_key) <= 0)
    {
        return Status::InvalidArgument("Keys must be added in strict ascending order");
    }

    r->builder->Add(key, value);
    Status s = r->builder->status();
    if (!s.ok())
        return s;
    r->file_info.largest_key.assign(key.data(), key.size());
    r->file_info.file_size = r->builder->FileSize();
    r->file_info.num_entries++;
    return s;
}

Status IndexFileWriter::Finish(IndexFileInfo * file_info)
{
    Rep * r = rep.get();
    if (!r->builder)
        return Status::InvalidArgument("File is not opened");
    if (r->file_info.num_entries == 0)
        return Status::InvalidArgument("Cannot create sst file with no entries");

    Status s = r->builder->Finish();
    r->file_info.file_size = r->builder->FileSize();

    if (s.ok())
    {
        s = r->file_writer->Sync();
        if (s.ok())
        {
            s = r->file_writer->Close();
        }
    }
    if (!s.ok())
        r->options.env->DeleteFile(r->file_info.file_path);

    r->file_info.file_hash = r->file_writer->getHash();
    if (file_info != nullptr)
        *file_info = r->file_info;
    r->builder.reset();
    return s;
}

}
