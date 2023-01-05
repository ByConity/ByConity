#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTSetQuery;

class ASTCreateWarehouseQuery: public IAST
{
public:

    String name;
    String getID(char) const override { return "CreateWarehouseQuery"; }

    ASTSetQuery * settings = nullptr;
    bool if_not_exists{false};

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

};
}
