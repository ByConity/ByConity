#pragma once


#include <Catalog/Catalog.h>
// #include <MergeTreeCommon/CnchServerClient.h>
#include <Common/Exception.h>
#include <Common/HostWithPorts.h>
#include <Interpreters/Context.h>
#include <Transaction/TxnTimestamp.h>

#include <Transaction/ICnchTransaction.h>


namespace DB
{
/// ProxyTransaction - A proxy to work with transaction on other server. 
/// - Alway `write` transaction (RO transaction can stand alone on any server w/o proxy)
/// - Not all method are supported. See comments in each the method detail information

class CnchProxyTransaction : public ICnchTransaction
{
    using Base = ICnchTransaction;

private:
    // CnchServerClientPtr remote_client;

public:
    explicit CnchProxyTransaction(Context & context_) : Base(context_) {}
    // explicit CnchProxyTransaction(Context & global_context, CnchServerClientPtr client, const TxnTimestamp & primary_txn_id);
    ~CnchProxyTransaction() override = default; 
    String getTxnType() const override { return "CnchProxyTransaction"; }
    void precommit() override;
    TxnTimestamp commit() override;
    TxnTimestamp commitV2() override;
    TxnTimestamp rollback() override;
    TxnTimestamp abort() override;
    void clean(TxnCleanTask & task) override;
    void cleanWrittenData() override;
    void syncTransactionStatus(bool throw_on_missmatch = false);
    void setTransactionStatus(CnchTransactionStatus status);
};

using ProxyTransactionPtr = std::shared_ptr<CnchProxyTransaction>;

}
