#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemKafkaTasks : public shared_ptr_helper<StorageSystemKafkaTasks>,
                                public IStorageSystemOneBlock<StorageSystemKafkaTasks>
{
public:
    std::string getName() const override { return "SystemKafkaTasks"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
#endif
