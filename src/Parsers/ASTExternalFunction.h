#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <DataTypes/IDataType.h>
#include <Functions/UserDefined/UDFFlags.h>

namespace DB
{
class ASTExternalFunction : public IAST
{
public:
    struct Arguments {
        UDFFlags flags {0};
        DataTypePtr ret;
        uint64_t version {0};
    };

    Arguments args;
    String body;
    String tag;

    ASTPtr clone() const override;

    String getID(char) const override { return "UDF"; }

    void formatImpl(const FormatSettings & s, FormatState & fmt, FormatStateStacked frame) const override;
};
}
