#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace
{
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    void formatNames(const Strings & names, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            settings.ostr << ' ' << backQuoteIfNeed(name);
        }
    }
}


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + toString(type) + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    return std::make_shared<ASTDropAccessEntityQuery>(*this);
}


void ASTDropAccessEntityQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << EntityTypeInfo::get(type).name
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    if (type == EntityType::ROW_POLICY)
    {
        settings.ostr << " ";
        row_policy_names->format(settings);
    }
    else
        formatNames(names, settings);

    formatOnCluster(settings);
}


void ASTDropAccessEntityQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabase(current_database);
}

void ASTDropAccessEntityQuery::rewriteNamesWithTenant(const Context *)
{
    if (!tenant_rewritten)
    {
        for (auto & name : names)
            name = formatTenantEntityName(name);
        if (row_policy_names)
        {
            for (auto & name_parts : row_policy_names->name_parts)
                name_parts.short_name = formatTenantEntityName(name_parts.short_name);
        }

        tenant_rewritten = true;
    }  
}
}
