#pragma once

#include <Core/Types.h>

namespace DB
{

struct FileInfo
{
    String file_path;
    bool is_directory;

    FileInfo(const String & file_path_, bool is_directory_) : file_path(file_path_), is_directory(is_directory_) { }
};

using FileInfos = std::vector<FileInfo>;

class FilePathMatcher
{
public:
    virtual ~FilePathMatcher() = default;

    Strings regexMatchFiles(const String & path_for_ls, const String & for_match);

    // For regex match, we remove scheme and prefix(S3 bucket) from full path.
    virtual String removeSchemeAndPrefix(const String & full_path);

protected:
    virtual FileInfos getFileInfos(const String & prefix_path) = 0;

    // For regex match, we remove scheme and prefix(S3 bucket) from full path.
    // But these prefix are needed when infile from some file system, so we will add it back.
    virtual String getSchemeAndPrefix() { return ""; }
};

}
