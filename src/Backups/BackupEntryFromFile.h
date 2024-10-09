#pragma once

#include <Backups/IBackupEntry.h>
#include <Disks/IDisk.h>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup,
class BackupEntryFromFile : public IBackupEntry
{
public:
    BackupEntryFromFile(const DiskPtr & disk_, const String & file_path_);

    BackupEntryType getEntryType() const override { return BackupEntryType::DATA; }

    size_t getSize() override
    {
        if (!file_size)
            file_size = disk->getFileSize(file_path);
        return *file_size;
    }

    String getFilePath() const { return file_path; }
    DiskPtr getDisk() const { return disk; }

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    const DiskPtr disk;
    const String file_path;
    std::optional<size_t> file_size;
};

}
