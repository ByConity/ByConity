/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <common/types.h>
#include <common/logger_useful.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class DiskLocalReservation;

class DiskLocal : public IDisk
{
public:
    friend class DiskLocalReservation;

    DiskLocal(const String & name_, const String & path_, const DiskStats & keep_free_disk_stats_)
        : name(name_), disk_path(path_), keep_free_disk_stats(keep_free_disk_stats_)
    {
        if (disk_path.back() != '/')
            throw Exception("Disk path must end with '/', but '" + disk_path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);
    }

    UInt64 getID() const override;

    const String & getName() const override { return name; }

    const String & getPath() const override { return disk_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    DiskStats getTotalSpace(bool with_keep_free = false) const override;

    DiskStats getAvailableSpace() const override;

    DiskStats getUnreservedSpace() const override;

    DiskStats getKeepingFreeSpace() const override { return keep_free_disk_stats; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings& settings) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        const WriteSettings& settings) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void truncateFile(const String & path, size_t size) override;

    DiskType::Type getType() const override { return DiskType::Type::Local; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

private:
    bool tryReserve(UInt64 bytes);

private:
    const String name;
    const String disk_path;
    const DiskStats keep_free_disk_stats;

    UInt64 reserved_bytes = 0;
    UInt64 reserved_inodes = 0; // TODO: placeholder and not implemented yet
    UInt64 reservation_count = 0;

    static std::mutex reservation_mutex;

    Poco::Logger * log = &Poco::Logger::get("DiskLocal");
};

}
