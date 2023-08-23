/*
 * Copyright 2023 Bytedance Ltd. and/or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
class S3DetachMetaAction : public IAction
{
public:
    S3DetachMetaAction(
        const ContextPtr & context_, const TxnTimestamp & txn_id_, const StoragePtr & tbl_, const MergeTreeDataPartsCNCHVector & parts_);

    virtual void executeV1(TxnTimestamp commit_time) override;
    virtual void executeV2() override;
    virtual void abort() override;
    virtual void postCommit(TxnTimestamp comit_time) override;

    static void commitByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources);
    static void abortByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources);

private:
    StoragePtr tbl;
    IMergeTreeDataPartsVector parts;
};

}
