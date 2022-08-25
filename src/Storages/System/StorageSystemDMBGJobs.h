#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

/* System table "dm_bg_jobs" for query info of bg job that managed in DaemonManager
*/
class StorageSystemDMBGJobs : public shared_ptr_helper<StorageSystemDMBGJobs>,
				   public DB::IStorageSystemOneBlock<StorageSystemDMBGJobs>
{
    friend struct shared_ptr_helper<StorageSystemDMBGJobs>;

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemBackgroundJobs"; }
    static NamesAndTypesList getNamesAndTypes();

};

}
