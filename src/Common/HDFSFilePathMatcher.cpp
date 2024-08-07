#include <filesystem>
#include <Common/HDFSFilePathMatcher.h>

namespace DB
{

HDFSFilePathMatcher::HDFSFilePathMatcher(String & path, const ContextPtr & context_ptr)
{
    Poco::URI uri(path);
    HDFSConnectionParams hdfs_params = context_ptr->getHdfsConnectionParams();
    HDFSBuilderPtr builder = hdfs_params.createBuilder(uri);
    hdfs_fs = createHDFSFS(builder.get());
}

FileInfos HDFSFilePathMatcher::getFileInfos(const String & prefix_path)
{
    FileInfos file_infos;
    HDFSFileInfo ls;
    ls.file_info = hdfsListDirectory(hdfs_fs.get(), prefix_path.data(), &ls.length);
    for (int i = 0; i < ls.length; i++)
    {
        file_infos.emplace_back(ls.file_info[i].mName, ls.file_info[i].mKind == kObjectKindDirectory);
    }
    return file_infos;
}

}
