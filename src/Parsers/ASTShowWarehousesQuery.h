#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTShowWarehousesQuery: public IAST
{
public:

    String getID(char) const override { return "ShowWarehousesQuery"; }
    String like;

    ASTPtr clone() const override;

    void formatLike(const FormatSettings & settings) const;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

};
}
