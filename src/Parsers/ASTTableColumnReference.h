#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
/// this internal AST is only used by optimizer.
/// Represent the origin table and column of a identifier.
class ASTTableColumnReference : public IAST
{
public:
    StoragePtr storage;
    String column_name;

    ASTTableColumnReference(StoragePtr storage_, String column_name_) : storage(std::move(storage_)), column_name(std::move(column_name_))
    {
    }

    String getID(char delim) const override;

    void appendColumnName(WriteBuffer &) const override;

    ASTType getType() const override { return ASTType::ASTTableColumnReference; }

    ASTPtr clone() const override { return std::make_shared<ASTTableColumnReference>(storage, column_name); }
};
}
