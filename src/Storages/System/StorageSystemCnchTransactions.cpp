#include <Storages/System/StorageSystemCnchTransactions.h>

#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TransactionCommon.h>
#include <Common/Exception.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
NamesAndTypesList StorageSystemCnchTransactions::getNamesAndTypes()
{
    return {
        {"txn_id", std::make_shared<DataTypeUInt64>()},
        {"create_time", std::make_shared<DataTypeDateTime>()},
        {"commit_ts", std::make_shared<DataTypeUInt64>()},
        {"status", std::make_shared<DataTypeString>()},
        {"priority", std::make_shared<DataTypeString>()},
        {"location", std::make_shared<DataTypeString>()},
        {"initiator", std::make_shared<DataTypeString>()},
        {"main_table_uuid", std::make_shared<DataTypeUUID>()},
        {"clean_ts", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemCnchTransactions::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    if (context->getServerType() != ServerType::cnch_server)
        throw Exception("Table system.cnch_transctions only support cnch server", ErrorCodes::NOT_IMPLEMENTED);

    std::vector<TxnTimestamp> txn_ids;
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    const std::vector<std::map<String,Field>> value_by_column_names = collectWhereORClausePredicate(where_expression, context);
    for (const auto & value_by_column_name : value_by_column_names)
    {
        auto txn_id_it = value_by_column_name.find("txn_id");
        if (txn_id_it != value_by_column_name.end())
        {
            txn_ids.emplace_back(txn_id_it->second.get<UInt64>());
        }
    }

    if (txn_ids.empty() && !context->getSettingsRef().allow_full_scan_txn_records)
        throw Exception("Table system.cnch_transctions must have a where clause with txn_id", ErrorCodes::NOT_IMPLEMENTED);

    std::vector<TransactionRecord> records;
    if (txn_ids.empty())
        records = context->getCnchCatalog()->getTransactionRecords();
    else
    {
        for (const auto & txn_id : txn_ids)
        {
            auto record = context->getCnchCatalog()->tryGetTransactionRecord(txn_id);
            if (record)
                records.emplace_back(std::move(*record));
        }
    }

    for (const auto & record : records)
    {
        size_t c = 0;
        res_columns[c++]->insert(UInt64(record.txnID()));
        res_columns[c++]->insert((UInt64(record.txnID()) >> 18) / 1000);
        res_columns[c++]->insert(UInt64(record.commitTs()));
        res_columns[c++]->insert(txnStatusToString(record.status()));
        res_columns[c++]->insert(txnPriorityToString(record.priority()));
        res_columns[c++]->insert(record.location());
        res_columns[c++]->insert(record.initiator());
        res_columns[c++]->insert(record.mainTableUUID());
        res_columns[c++]->insert(UInt64(record.cleanTs()));
    }
}

}
