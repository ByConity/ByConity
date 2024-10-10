#include <DataStreams/TransactionWrapperBlockInputStream.h>
#include <common/logger_useful.h>

namespace DB
{
TransactionWrapperBlockInputStream::TransactionWrapperBlockInputStream(
    const BlockInputStreamPtr & input, TransactionCnchPtr txn_, std::shared_ptr<CnchLockHolder> lock_holder_)
    : txn(std::move(txn_)), lock_holder(std::move(lock_holder_))
{
    children.emplace_back(input);
}

Block TransactionWrapperBlockInputStream::readImpl()
{
    auto block = children.back()->read();
    return block;
}

void TransactionWrapperBlockInputStream::readSuffixImpl()
{
    txn->commitV2();
    if (lock_holder)
        lock_holder->unlock();
}

}
