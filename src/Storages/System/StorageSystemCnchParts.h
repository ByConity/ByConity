#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <vector>
#include <Core/Field.h>

namespace DB
{

class Context;

class StorageSystemCnchParts : public shared_ptr_helper<StorageSystemCnchParts>, public IStorageSystemOneBlock<StorageSystemCnchParts>
{
public:

    std::string getName() const override
    {
        return "SystemCnchParts";
    }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases();

    enum PartType
    {
        VisiblePart = 1,
        InvisiblePart = 2,
        DropRange = 3,
        DroppedPart = 4,
    };

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
