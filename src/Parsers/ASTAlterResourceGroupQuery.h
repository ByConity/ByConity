#pragma once

#include <Parsers/IAST.h>
#include <Core/Field.h>

namespace DB
{

class ASTResourceGroupSetting : public IAST
{
public:
    String setting_name;
    Field value;

    String getID(char) const override { return "ResourceGroupSetting"; }
    ASTPtr clone() const override { return std::make_shared<ASTResourceGroupSetting>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

/** ALTER RESOURCE GROUP name
 *      SETTINGS [variable = value] [,...]
 */
class ASTAlterResourceGroupQuery : public IAST
{
public:
    String name;
    std::vector<std::shared_ptr<ASTResourceGroupSetting>> group_settings;

    bool empty() const { return group_settings.empty(); }
    String getID(char) const override { return "AlterResourceGroupQuery"; }
    ASTPtr clone() const override { return std::make_shared<ASTAlterResourceGroupQuery>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
