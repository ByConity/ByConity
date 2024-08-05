#pragma once

#include <Interpreters/Context.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Common/FilePathMatcher.h>

namespace DB
{

class HDFSFilePathMatcher : public FilePathMatcher
{
public:
    HDFSFilePathMatcher(String & path, const ContextPtr & context_ptr);

    ~HDFSFilePathMatcher() override = default;

    FileInfos getFileInfos(const String & prefix_path) override;

private:
    HDFSFSPtr hdfs_fs;
};

}
