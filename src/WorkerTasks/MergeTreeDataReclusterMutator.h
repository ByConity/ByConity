#pragma once

#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <WorkerTasks/ManipulationTask.h>


namespace DB
{

class MergeTreeDataReclusterMutator
{

public:
    MergeTreeDataReclusterMutator(MergeTreeMetaBase & data_);

    MergeTreeMutableDataPartsVector executeClusterTask(
        const ManipulationTaskParams & params,
        ManipulationListEntry & manipulation_entry,
        ContextPtr context);


private:

    MergeTreeMutableDataPartsVector executeOnSinglePart(
        const MergeTreeDataPartPtr & part,
        const ManipulationTaskParams & params,
        ManipulationListEntry & manipulation_entry,
        ContextPtr context);

    bool checkOperationIsNotCanceled(const ManipulationListEntry & manipulation_entry) const;

    MergeTreeMetaBase & data;

    Poco::Logger * log;
};

}
