#pragma once

#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>

namespace DB
{
class IStorageDataLake : public IStorage, WithContext
{
public:
    IStorageDataLake(const StorageID & table_id, ContextPtr context_) : IStorage(table_id), WithContext(context_) { }

    
};

}
