#pragma once

#include <Core/Types.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct QualifiedName
{
    std::vector<String> parts;

    QualifiedName() = default;
    explicit QualifiedName(std::vector<String> parts_) : parts(std::move(parts_)) {}
    QualifiedName(std::initializer_list<String> parts_) : parts(parts_) {}

    static QualifiedName extractQualifiedName(const ASTIdentifier & identifier);
    static QualifiedName extractQualifiedName(const DatabaseAndTableWithAlias & identifier);

    bool empty() const {return parts.empty();}
    const String & getLast() const {return parts.back();}
    QualifiedName getPrefix() const;
    bool hasSuffix(const QualifiedName & suffix) const;
    String toString() const;
};

}
