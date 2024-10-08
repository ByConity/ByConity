#pragma once

#include <Core/Types.h>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{
class ReadBuffer;

/// A backup entry represents some data which should be written to the backup or has been read from the backup.
class IBackupEntry
{
public:
    enum class BackupEntryType
    {
        // Metadata have to write to a new file
        METADATA = 0,
        // DATA can copy from source disk to destination
        DATA = 1,
    };

    virtual ~IBackupEntry() = default;

    virtual BackupEntryType getEntryType() const = 0;

    /// Returns the size of the data.
    virtual size_t getSize() = 0;

    /// Returns the checksum of the data if it's precalculated.
    /// Can return nullopt which means the checksum should be calculated from the read buffer.
    virtual std::optional<UInt128> getChecksum() const { return {}; }

    /// Returns a read buffer for reading the data.
    virtual std::unique_ptr<ReadBuffer> getReadBuffer() const = 0;
};

using BackupEntryPtr = std::unique_ptr<IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;

}
