#pragma once

#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <map>

namespace DB
{

class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;
using DisksMap = std::map<String, DiskPtr>;
using DiskIDMap = std::map<UInt64, DiskPtr>;
using DisksInfo = std::map<UInt64, std::pair<String, String>>;

/// Parse .xml configuration and store information about disks
/// Mostly used for introspection.
class DiskSelector
{
public:
    DiskSelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);
    DiskSelector(const DiskSelector & from) : disks(from.disks), id_to_disks(from.id_to_disks) { }

    DiskSelectorPtr updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context
    ) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    DiskPtr getByID(const UInt64 & disk_id) const;

    /// Get all disks with names
    const DisksMap & getDisksMap() const { return disks; }
    void addToDiskMap(String name, DiskPtr disk);

    /// save information of current disks into file.
    void flushDiskInfo() const;

    /// load disk info from file. help to check uniqueness of disk id 
    DisksInfo loadDiskInfo() const;


private:
    fs::path disks_path;
    DisksMap disks;
    DiskIDMap id_to_disks;
};

}
