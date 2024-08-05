#pragma once

#include <Common/FilePathMatcher.h>

namespace DB
{

class LocalFilePathMatcher : public FilePathMatcher
{
public:
    ~LocalFilePathMatcher() override = default;

    FileInfos getFileInfos(const String & prefix_path) override;
};

}
