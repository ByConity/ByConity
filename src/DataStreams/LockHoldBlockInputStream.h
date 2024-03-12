#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Transaction/CnchLock.h>

namespace DB
{

class LockHoldBlockInputStream : public IBlockInputStream
{
public:
    LockHoldBlockInputStream(
        const BlockInputStreamPtr & input_,
        CnchLockHolderPtrs && lock_holders_
    );

    String getName() const override { return "LockHoldBlockInputStream"; }

    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override;
private:
    void readSuffixImpl() override;
    CnchLockHolderPtrs lock_holders;
};

}
