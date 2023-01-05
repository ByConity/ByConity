#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTSetQuery;

class ASTAlterWarehouseQuery: public IAST
{
public:

    String name;
    String rename_to;
    String getID(char) const override { return "AlterWarehouseQuery"; }

    ASTSetQuery * settings = nullptr;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

};
}
