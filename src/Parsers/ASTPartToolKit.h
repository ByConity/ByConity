#pragma once

#include <Parsers/IAST.h>

namespace DB
{

enum class PartToolType
{
    NOTYPE = 0,
    WRITER = 0,
    MERGER = 1,
    CONVERTOR = 2
};

class ASTPartToolKit : public IAST
{
public:
    ASTPtr data_format;
    ASTPtr create_query;
    ASTPtr source_path;
    ASTPtr target_path;
    ASTPtr settings;

    PartToolType type = PartToolType::NOTYPE;

    String getID(char) const override;

    ASTType getType() const override { return ASTType::ASTPartToolKit; }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
