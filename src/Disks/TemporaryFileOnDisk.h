#pragma once

#include <Core/Types.h>
#include <memory>
#include <filesystem>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// This class helps with the handling of temporary files or directories.
/// A unique name for the temporary file or directory is automatically chosen based on a specified prefix.
/// Optionally can create a directory in the constructor.
/// The destructor always removes the temporary file or directory with all contained files.
class TemporaryFileOnDisk
{
public:
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_ = "tmp");
    ~TemporaryFileOnDisk();

    DiskPtr getDisk() const { return disk; }
    const String & getPath() const { return filepath; }
    String getAbsolutePath() const;

private:
    DiskPtr disk;
    String filepath;
};

using TemporaryFileOnDiskHolder = std::unique_ptr<TemporaryFileOnDisk>;

static const double kDefaultSpillTrigerThreshold = 0.7;

}
