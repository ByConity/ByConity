#pragma once

#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>

namespace DB
{
class JNIScanInfo : public ILakeScanInfo
{
protected:
    explicit JNIScanInfo(const ILakeScanInfo::StorageType storage_type_, const size_t distribution_id_) : ILakeScanInfo(storage_type_)
    {
        distribution_id = distribution_id_;
    }
};
}
