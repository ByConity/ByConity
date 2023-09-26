#pragma once

#include <Parsers/ASTIdentifier.h>

namespace DB
{
struct FkToPKPair;
using ForeignKeysTuple = std::vector<FkToPKPair>;
struct ForeignKeysDescription
{
public:
    ForeignKeysDescription() = default;
    explicit ForeignKeysDescription(const ASTs & foreign_keys_);
    ForeignKeysDescription(const ForeignKeysDescription & other);
    ForeignKeysDescription & operator=(const ForeignKeysDescription & other);

    ForeignKeysDescription(ForeignKeysDescription && other) noexcept;
    ForeignKeysDescription & operator=(ForeignKeysDescription && other) noexcept;

    bool empty() const
    {
        return foreign_keys.empty();
    }
    String toString() const;

    static ForeignKeysDescription parse(const String & str);

    ForeignKeysTuple getForeignKeysTuple() const;

    const ASTs & getForeignKeys() const;
    ASTs foreign_keys;
};

}
