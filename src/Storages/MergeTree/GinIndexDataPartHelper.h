#pragma once 


#include <memory>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/DiskCache_fwd.h>
#include "Core/SettingsEnums.h"
#include "Disks/IDisk.h"
#include "Storages/DiskCache/DiskCacheLRU.h"

namespace DB
{

class IGinDataPartHelper
{
public:
    virtual ~IGinDataPartHelper() = default;

    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String& file_name) = 0;
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String& file_name, size_t buf_size, WriteMode write_mode) = 0;

    virtual size_t getFileSize(const String& file_name) const = 0;

    virtual String getPartUniqueID() const = 0;

    virtual bool exists(const String& file_name) const = 0;
};

class GinDataLocalPartHelper: public IGinDataPartHelper
{
public:
    explicit GinDataLocalPartHelper(const IMergeTreeDataPart& part_);
    GinDataLocalPartHelper(const DiskPtr& disk_, const String& relative_path_);

    std::unique_ptr<SeekableReadBuffer> readFile(const String& file_name) override;
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String& file_name,
        size_t buf_size, WriteMode write_mode) override;

    size_t getFileSize(const String& file_name) const override;

    String getPartUniqueID() const override;

    bool exists(const String& file_name) const override;

private:
    DiskPtr disk;
    String relative_path;
};

class GinDataCNCHPartHelper: public IGinDataPartHelper
{
public:
    GinDataCNCHPartHelper(const IMergeTreeDataPartPtr& part_,
        const IDiskCachePtr& cache_, DiskCacheMode mode_ = DiskCacheMode::AUTO);

    std::unique_ptr<SeekableReadBuffer> readFile(const String& file_name) override;
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String& file_name,
        size_t buf_size, WriteMode write_mode) override;

    size_t getFileSize(const String& file_name) const override;

    String getPartUniqueID() const override;

    bool exists(const String& file_name) const override;

private:
    IDiskCachePtr cache;

    IMergeTreeDataPart::ChecksumsPtr part_checksums;

    DiskPtr disk;
    String part_rel_path;

    DiskCacheMode mode;

    Poco::Logger * log;
};

}
