#include <Parsers/ASTSetRoleQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>


namespace DB
{
String ASTSetRoleQuery::getID(char) const
{
    return "SetRoleQuery";
}


ASTPtr ASTSetRoleQuery::clone() const
{
    return std::make_shared<ASTSetRoleQuery>(*this);
}


void ASTSetRoleQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    switch (kind)
    {
        case Kind::SET_ROLE: settings.ostr << "SET ROLE"; break;
        case Kind::SET_ROLE_DEFAULT: settings.ostr << "SET ROLE DEFAULT"; break;
        case Kind::SET_DEFAULT_ROLE: settings.ostr << "SET DEFAULT ROLE"; break;
    }
    settings.ostr << (settings.hilite ? hilite_none : "");

    if (kind == Kind::SET_ROLE_DEFAULT)
        return;

    settings.ostr << " ";
    roles->format(settings);

    if (kind == Kind::SET_ROLE)
        return;

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "");
    to_users->format(settings);
}

void ASTSetRoleQuery::rewriteNamesWithTenant(const Context *)
{
    if (!tenant_rewritten)
    {
        if (roles)
        {
            for (auto & name : roles->names)
                name = formatTenantEntityName(name);
            for (auto & name : roles->except_names)
                name = formatTenantEntityName(name);
        }
        if (to_users)
        {
            for (auto & name : to_users->names)
                name = formatTenantEntityName(name);
            for (auto & name : to_users->except_names)
                name = formatTenantEntityName(name);
        }

        tenant_rewritten = true;
    }  
}
}
