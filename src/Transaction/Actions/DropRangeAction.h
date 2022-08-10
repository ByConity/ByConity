#pragma once

#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Transaction/Actions/IAction.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Transaction/TransactionCommon.h>

namespace DB
{

class DropRangeAction : public IAction
{
public:
    DropRangeAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, TransactionRecord record, const StoragePtr table_)
    :
    IAction(query_context_, txn_id_),
    txn_record(std::move(record)),
    table(table_),
    log(&Poco::Logger::get("DropRangeAction"))
    {}

    ~DropRangeAction() override = default;

    void appendPart(MutableMergeTreeDataPartCNCHPtr part);
    void appendDeleteBitmap(DeleteBitmapMetaPtr delete_bitmap);

    /// v1 APIs
    void executeV1(TxnTimestamp commit_time) override;

    /// V2 APIs
    void executeV2() override;
    void postCommit(TxnTimestamp commit_time) override;
    void abort() override;

    UInt32 getSize() const override { return parts.size() + delete_bitmaps.size(); }
private:
    TransactionRecord txn_record;
    const StoragePtr table;
    Poco::Logger * log;

    MutableMergeTreeDataPartsCNCHVector parts;
    DeleteBitmapMetaPtrVector delete_bitmaps;
};

}

