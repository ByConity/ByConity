#pragma once

#include <vector>
#include <Core/Field.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class Context;

class StorageSystemCnchUDF : public shared_ptr_helper<StorageSystemCnchUDF>, public IStorageSystemOneBlock<StorageSystemCnchUDF>
{
public:
    std::string getName() const override { return "SystemCnchUDF"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
