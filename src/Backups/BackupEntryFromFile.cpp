#include <Backups/BackupEntryFromFile.h>
#include <Disks/IDisk.h>
#include <IO/ReadHelpers.h>
#include <IO/createReadBufferFromFileBase.h>


namespace DB
{

BackupEntryFromFile::BackupEntryFromFile(const DiskPtr & disk_, const String & file_path_) : disk(disk_), file_path(file_path_)
{
}

std::unique_ptr<ReadBuffer> BackupEntryFromFile::getReadBuffer() const
{
    return disk->readFile(file_path);
}

}
