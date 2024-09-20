#pragma once

#include <IO/S3Common.h>
#include <Interpreters/Context.h>
#include <Common/FilePathMatcher.h>

namespace DB
{
const static String S3_SCHEME = "s3://";

class S3FilePathMatcher : public FilePathMatcher
{
public:
    S3FilePathMatcher(const String & path, const ContextPtr & context_ptr);

    ~S3FilePathMatcher() override = default;

    FileInfos getFileInfos(const String & prefix_path) override;

    String getSchemeAndPrefix() override;

    String removeSchemeAndPrefix(const String & full_path) override;

private:
    std::unique_ptr<S3::S3Util> s3_util;
};

}
