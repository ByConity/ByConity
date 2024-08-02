#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemCnchTableTransactions.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
NamesAndTypesList StorageSystemCnchTableTransactions::getNamesAndTypes()
{
    return {
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"txn_id", std::make_shared<DataTypeUInt64>()},
        {"create_time", std::make_shared<DataTypeDateTime>()},
    };
}

void StorageSystemCnchTableTransactions::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & /*query_info*/) const
{
    if (context->getServerType() != ServerType::cnch_server)
        throw Exception("Table system.cnch_table_transctions only support cnch server", ErrorCodes::NOT_IMPLEMENTED);

    auto table_to_xids = context->getCnchTransactionCoordinator().getActiveXIDsPerTable();
    for (const auto & pair : table_to_xids)
    {
        for (const TxnTimestamp & xid : pair.second)
        {
            size_t col = 0;
            res_columns[col++]->insert(pair.first);
            res_columns[col++]->insert(xid.toUInt64());
            res_columns[col++]->insert(xid.toSecond());
        }
    }
}
}
