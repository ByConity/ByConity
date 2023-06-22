#include <DataStreams/TransactionWrapperBlockInputStream.h>

namespace DB
{

TransactionWrapperBlockInputStream::TransactionWrapperBlockInputStream(
    const BlockInputStreamPtr & input,
    TransactionCnchPtr txn_)
    : txn(std::move(txn_))
{
    children.emplace_back(input);
}

Block TransactionWrapperBlockInputStream::readImpl()
{
    return children.back()->read();
}

void TransactionWrapperBlockInputStream::readSuffixImpl()
{
    txn->commitV2();
}

}
