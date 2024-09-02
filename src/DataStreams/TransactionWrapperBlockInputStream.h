#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Transaction/CnchLock.h>
#include <Transaction/ICnchTransaction.h>

namespace DB
{

class TransactionWrapperBlockInputStream : public IBlockInputStream
{
public:
    TransactionWrapperBlockInputStream(
        const BlockInputStreamPtr & input_, TransactionCnchPtr txn_, std::shared_ptr<CnchLockHolder> lock_holder_ = {});

    String getName() const override { return "TransactionWrapperBlockInputStream"; }

    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override;
private:
    void readSuffixImpl() override;

    TransactionCnchPtr txn;
    std::shared_ptr<CnchLockHolder> lock_holder;
};

}
