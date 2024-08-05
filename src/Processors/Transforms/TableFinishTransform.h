#pragma once
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/IProcessor.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/CnchLock.h>
#include "Processors/Sources/SourceWithProgress.h"

namespace DB
{

class TableFinishTransform : public IProcessor
{
public:
    TableFinishTransform(
        const Block & header_,
        const StoragePtr & storage_,
        const ContextPtr & context_,
        ASTPtr & query_,
        bool insert_select_with_profiles_ = false);

    String getName() const override
    {
        return "TableFinish";
    }

    Status prepare() override;
    void work() override;

    InputPort & getInputPort()
    {
        return input;
    }
    OutputPort & getOutputPort()
    {
        return output;
    }

    void setProcessListElement(QueryStatus * elem);
    void setProgressCallback(const ProgressCallback & callback)
    {
        progress_callback = callback;
    }

private:
    void consume(Chunk block);
    void onFinish();
    Block getHeader();

    InputPort & input;
    OutputPort & output;

    Block header;

    Chunk current_output_chunk;
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;

    ProgressCallback progress_callback;
    QueryStatus * process_list_elem = nullptr;

    StoragePtr storage;
    ContextPtr context;
    ASTPtr query;
    bool insert_select_with_profiles;
    CnchLockHolderPtrs lock_holders;
};

}
