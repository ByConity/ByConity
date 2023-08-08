#include <memory>
#include <Interpreters/InterpreterShowStatementsQuery.h>
#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Transaction/CnchExplicitTransaction.h>
#include <Transaction/TransactionCommon.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

NamesAndTypes InterpreterShowStatementsQuery::getBlockStructure()
{
    return
    {
        {"txn_id", std::make_shared<DataTypeUInt64>()},
        {"statement", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeString>()},
    };
}

BlockIO InterpreterShowStatementsQuery::execute()
{
    if (!isQueryInInteractiveSession(getContext(), query_ptr))
    {
        throw Exception("SHOW STATEMENTS are supported only in interactive sessions", ErrorCodes::LOGICAL_ERROR);
    }
    auto * explicit_txn = getContext()->getSessionContext()->getCurrentTransaction()->as<CnchExplicitTransaction>();
    const auto & statements = explicit_txn->getStatements();
    const auto & txns = explicit_txn->getSecondaryTransactions();
    assert(statements.size() == txns.size());
    Block block;
    auto block_structures = getBlockStructure();
    for (auto & [name, type] : block_structures)
    {
        block.insert({type->createColumn(), type, name});
    }
    MutableColumns res_columns = block.cloneEmptyColumns();
    for (size_t i = 0; i < statements.size(); ++i)
    {
        res_columns[0]->insert(txns[i]->getTransactionID().toUInt64());
        res_columns[1]->insert(statements[i]);
        res_columns[2]->insert(txnStatusToString(txns[i]->getStatus()));
    }
    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(block.cloneWithColumns(std::move(res_columns)));
    return res;
}
}
