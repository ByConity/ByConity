#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTSetQuery;

class ASTCreateWorkerGroupQuery : public IAST
{
public:
    String getID(char) const override { return "CreateWorkerGroupQuery"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override;

    bool if_not_exists{false};
    String worker_group_id;
    String vw_name;
    ASTSetQuery * settings{nullptr};
};

}
