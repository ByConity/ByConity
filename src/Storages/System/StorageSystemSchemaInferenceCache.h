#pragma once

#include <Storages/Cache/SchemaCache.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemSchemaInferenceCache final : public shared_ptr_helper<StorageSystemSchemaInferenceCache>, public IStorageSystemOneBlock<StorageSystemSchemaInferenceCache>
{
public:
    std::string getName() const override { return "StorageSystemSchemaInferenceCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemSchemaInferenceCache>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;

    void fillDataImpl(MutableColumns & res_columns, SchemaCache & schema_cache, const String & storage_name) const;

};

}
