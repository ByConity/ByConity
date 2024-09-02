#include <DataStreams/LockHoldBlockInputStream.h>

namespace DB
{

LockHoldBlockInputStream::LockHoldBlockInputStream(
    const BlockInputStreamPtr & input,
    CnchLockHolderPtrs && lock_holders_)
    : lock_holders(std::move(lock_holders_))
{
    if (input)
        children.emplace_back(input);
}

Block LockHoldBlockInputStream::readImpl()
{
    if (children.empty())
        return {};
    return children.back()->read();
}

void LockHoldBlockInputStream::readSuffixImpl()
{
    lock_holders.clear();
}

}
