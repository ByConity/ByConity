#include <Analyzers/QualifiedName.h>

namespace DB
{

QualifiedName QualifiedName::extractQualifiedName(const ASTIdentifier & identifier)
{
    return QualifiedName(identifier.nameParts());
}

QualifiedName QualifiedName::extractQualifiedName(const DatabaseAndTableWithAlias & db_and_table)
{
    if (!db_and_table.alias.empty())
        return {db_and_table.alias};
    else if (!db_and_table.database.empty())
        return {db_and_table.database, db_and_table.table};
    else if (!db_and_table.table.empty())
        return {db_and_table.table};
    else
        return {};
}

QualifiedName QualifiedName::getPrefix() const
{
    if (this->empty())
        throw Exception("Can not get prefix for an empty qualified name.", ErrorCodes::LOGICAL_ERROR);

    std::vector<String> prefix_parts {parts.begin(), parts.end() - 1};
    return QualifiedName(prefix_parts);
}

bool QualifiedName::hasSuffix(const QualifiedName & suffix) const
{
    if (suffix.empty())
        return true;
    else if (this->empty())
        return false;
    else
        return this->getLast() == suffix.getLast() && getPrefix().hasSuffix(suffix.getPrefix());
}

String QualifiedName::toString() const
{
    String str;
    for (const auto & part: parts)
    {
        if (!str.empty())
            str += '.';
        str += part;
    }
    return str;
}

}
