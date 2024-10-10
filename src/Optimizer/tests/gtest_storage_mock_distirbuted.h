#pragma once

#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>

namespace DB
{

class StorageMockDistributed final : public shared_ptr_helper<StorageMockDistributed>, public IStorage
{
    friend struct shared_ptr_helper<StorageMockDistributed>;

public:
    static constexpr auto ENGINE_NAME = "MockDistributed";

    String getName() const override { return ENGINE_NAME; }

    bool supportsOptimizer() const override { return true; }
    bool supportsDistributedRead() const override { return true; }

    bool isBucketTable() const override { return getInMemoryMetadataPtr()->isClusterByKeyDefined(); }
    bool isTableClustered(ContextPtr /* context*/) const override { return isBucketTable(); }

protected:
    StorageMockDistributed(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        ForeignKeysDescription foreign_keys_,
        UniqueNotEnforcedDescription unique_not_enforced_,
        ASTStorage * storage_def,
        const String & comment,
        ContextPtr context_)
        : IStorage(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(std::move(columns_description_));
        storage_metadata.setConstraints(std::move(constraints_));
        storage_metadata.setForeignKeys(std::move(foreign_keys_));
        storage_metadata.setUniqueNotEnforced(std::move(unique_not_enforced_));
        storage_metadata.setComment(comment);
        if (storage_def->cluster_by)
            storage_metadata.cluster_by_key
                = KeyDescription::getClusterByKeyFromAST(storage_def->cluster_by->ptr(), storage_metadata.columns, context_);
        setInMemoryMetadata(storage_metadata);
    }
};

void registerStorageMockDistributedDistirubted(StorageFactory & factory)
{
    factory.registerStorage(
        StorageMockDistributed::ENGINE_NAME,
        [](const StorageFactory::Arguments & args) {
            return StorageMockDistributed::create(
                args.table_id,
                args.columns,
                args.constraints,
                args.foreign_keys,
                args.unique,
                args.storage_def,
                args.comment,
                args.getContext());
        },
        {.supports_settings = true, .supports_sort_order = true});
}

inline void tryRegisterStorageMockDistributed()
{
    static struct Register
    {
        Register()
        {
            auto & factory = StorageFactory::instance();
            registerStorageMockDistributedDistirubted(factory);
        }
    } registered;
}

}
