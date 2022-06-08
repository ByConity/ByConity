#include <Parsers/ASTAlterResourceGroupQuery.h>
#include <IO/Operators.h>
#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

void ASTResourceGroupSetting::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << setting_name
                  << " = "
                  << applyVisitor(FieldVisitorToString(), value);
}

void ASTAlterResourceGroupQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "ALTER RESOURCE GROUP " << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << name << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " SETTINGS " << (settings.hilite ? hilite_none : "");

    if (empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "NONE" << (settings.hilite ? hilite_none : "");
        return;
    }

    bool needComma = false;
    for (const auto & setting : group_settings)
    {
        if (needComma)
            settings.ostr << ", ";
        needComma = true;

        setting->format(settings);
    }
}

}
