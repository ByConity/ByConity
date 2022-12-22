#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTDropWarehouseQuery: public IAST
{
public:

    String name;
    bool if_exists{false};
    String getID(char) const override { return "DropWarehouseQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

};
}
