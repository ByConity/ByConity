#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IDisk.h>
#include <Poco/TemporaryFile.h>


namespace DB
{

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_)
    : disk(disk_)
{
    if (fs::path prefix_path(prefix_); prefix_path.has_parent_path())
        disk->createDirectories(prefix_path.parent_path());

    String dummy_prefix = "a/";
    filepath = Poco::TemporaryFile::tempName(dummy_prefix);
    dummy_prefix += "tmp";
    assert(filepath.starts_with(dummy_prefix));
    filepath.replace(0, dummy_prefix.length(), prefix_);
}

String TemporaryFileOnDisk::getAbsolutePath() const
{
    return std::filesystem::path(disk->getPath()) / filepath;
}

TemporaryFileOnDisk::~TemporaryFileOnDisk()
{
#if 1
    try
    {
        if (disk && !filepath.empty())
            disk->removeRecursive(filepath);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
#endif
}

}
