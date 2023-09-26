#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Core/Names.h>

namespace DB
{

struct UniqueNotEnforcedDescription
{
public:
    UniqueNotEnforcedDescription() = default;
    explicit UniqueNotEnforcedDescription(const ASTs & unique_keys_);
    UniqueNotEnforcedDescription(const UniqueNotEnforcedDescription & other);
    UniqueNotEnforcedDescription & operator=(const UniqueNotEnforcedDescription & other);

    UniqueNotEnforcedDescription(UniqueNotEnforcedDescription && other) noexcept;
    UniqueNotEnforcedDescription & operator=(UniqueNotEnforcedDescription && other) noexcept;

    bool empty() const
    {
        return unique.empty();
    }
    String toString() const;

    static UniqueNotEnforcedDescription parse(const String & str);

    std::vector<Names> getUniqueNames() const;
    const std::vector<ASTPtr> & getUnique() const;

    ASTs unique;
};

}
