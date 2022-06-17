#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTSetQuery;

class ASTDropWorkerGroupQuery : public IAST
{
public:
    String getID(char) const override { return "DropWorkerGroupQuery"; }


    ASTPtr clone() const override;
    void formatImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override;

    bool if_exists{false};
    String worker_group_id;
    ASTSetQuery * settings{nullptr};
};

}
