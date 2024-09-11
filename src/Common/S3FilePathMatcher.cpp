#include <filesystem>
#include <Common/S3FilePathMatcher.h>

namespace DB
{

S3FilePathMatcher::S3FilePathMatcher(const String & path, const ContextPtr & context_ptr)
{
    const auto & settings = context_ptr->getSettingsRef();
    S3::URI s3_uri(path);
    String endpoint = !s3_uri.endpoint.empty() ? s3_uri.endpoint : settings.s3_endpoint.toString();
    S3::S3Config s3_cfg(
        endpoint,
        settings.s3_region.toString(),
        s3_uri.bucket,
        settings.s3_ak_id.toString(),
        settings.s3_ak_secret.toString(),
        "",
        "",
        settings.s3_use_virtual_hosted_style);
    const std::shared_ptr<Aws::S3::S3Client> client = s3_cfg.create();
    s3_util = std::make_unique<S3::S3Util>(client, s3_uri.bucket, false);
}


FileInfos S3FilePathMatcher::getFileInfos(const String & prefix_path)
{
    FileInfos file_infos;

    // erase '/' at first to list objects in the bucket
    String prefix_without_slash = prefix_path;
    size_t pos = prefix_without_slash.find_first_not_of('/');
    if (pos != std::string::npos)
        prefix_without_slash.erase(0, pos);
    else
        prefix_without_slash.clear();

    S3::S3Util::S3ListResult s3_list_result = s3_util->listObjectsWithDelimiter(prefix_without_slash, "/", false);

    if (s3_list_result.object_names.empty())
        return file_infos;

    int ls_length = s3_list_result.object_names.size();
    for (int i = 0; i < ls_length; i++)
    {
        // add '/' at first to keep the same with other file system
        String file_path = std::filesystem::path("/") / s3_list_result.object_names[i];
        file_infos.emplace_back(file_path, s3_list_result.is_common_prefix[i]);
    }

    return file_infos;
}

String S3FilePathMatcher::getSchemeAndPrefix()
{
    return S3_SCHEME + s3_util->getBucket();
}

String S3FilePathMatcher::removeSchemeAndPrefix(const String & full_path)
{
    // remove scheme and bucket from path, add '/' at first to keep the same with other file system
    return std::filesystem::path("/") / S3::URI(full_path).key;
}
}
