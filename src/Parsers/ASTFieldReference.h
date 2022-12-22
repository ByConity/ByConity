#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{

/// this AST is only used by optimizer.
class ASTFieldReference : public ASTWithAlias
{
public:
    size_t field_index;

    explicit ASTFieldReference(size_t field_index_): field_index(field_index_)
    {}

    String getID(char delim) const override { return "FieldRef" + (delim + std::to_string(field_index)); }

    ASTType getType() const override { return ASTType::ASTFieldReference; }

    ASTPtr clone() const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
