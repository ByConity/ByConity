#pragma once

#include <memory>

#include <Core/Types.h>
#include <Storages/IndexFile/Options.h>
#include <Storages/IndexFile/Status.h>
#include <Common/Slice.h>

namespace DB::IndexFile
{
class IndexFileReader
{
public:
    explicit IndexFileReader(const Options & options);

    ~IndexFileReader();

    /// Prepares to read from a local file located at "file_path".
    Status Open(const String & file_path);

    /// Prepares to read from "remote_file".
    Status Open(const RemoteFileInfo & remote_file);

    /// If the file contains an entry for "key" store the
    /// corresponding value in *value and return OK.
    ///
    /// If there is no entry for "key" leave *value unchanged and return
    /// a status for which Status::IsNotFound() returns true.
    ///
    /// May return some other Status on an error.
    Status Get(const ReadOptions & options, const Slice & key, String * value);

    /// Return bytes of resident memory usage.
    size_t ResidentMemoryUsage() const;

private:
    struct Rep;
    std::unique_ptr<Rep> rep;
};

}
