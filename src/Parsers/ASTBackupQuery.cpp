#include <IO/Operators.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <boost/range/algorithm_ext/erase.hpp>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    void formatPartitions(const ASTs & partitions, const IAST::FormatSettings & format)
    {
        format.ostr << " " << (format.hilite ? IAST::hilite_keyword : "") << ((partitions.size() == 1) ? "PARTITION" : "PARTITIONS") << " "
                    << (format.hilite ? IAST::hilite_none : "");
        bool need_comma = false;
        for (const auto & partition : partitions)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            partition->format(format);
        }
    }

    void formatExceptTables(const std::set<DatabaseAndTableName> & except_tables, const IAST::FormatSettings & format)
    {
        if (except_tables.empty())
            return;

        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " EXCEPT " << (except_tables.size() == 1 ? "TABLE" : "TABLES") << " "
                    << (format.hilite ? IAST::hilite_none : "");

        bool need_comma = false;
        for (const auto & table_name : except_tables)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ", ";

            if (!table_name.first.empty())
                format.ostr << backQuoteIfNeed(table_name.first) << ".";
            format.ostr << backQuoteIfNeed(table_name.second);
        }
    }

    void formatElement(const Element & element, const IAST::FormatSettings & format)
    {
        switch (element.type)
        {
            case ElementType::TABLE: {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << "TABLE " << (format.hilite ? IAST::hilite_none : "");

                if (!element.database_name.empty())
                    format.ostr << backQuoteIfNeed(element.database_name) << ".";
                format.ostr << backQuoteIfNeed(element.table_name);

                if ((element.new_table_name != element.table_name) || (element.new_database_name != element.database_name))
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                    if (!element.new_database_name.empty())
                        format.ostr << backQuoteIfNeed(element.new_database_name) << ".";
                    format.ostr << backQuoteIfNeed(element.new_table_name);
                }

                if (element.partitions)
                    formatPartitions(*element.partitions, format);

                break;
            }


            case ElementType::DATABASE: {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "");
                format.ostr << "DATABASE ";
                format.ostr << (format.hilite ? IAST::hilite_none : "");
                format.ostr << backQuoteIfNeed(element.database_name);

                if (element.new_database_name != element.database_name)
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                    format.ostr << backQuoteIfNeed(element.new_database_name);
                }

                formatExceptTables(element.except_tables, format);
                break;
            }
        }
    }

    void formatElements(const std::vector<Element> & elements, const IAST::FormatSettings & format)
    {
        bool need_comma = false;
        for (const auto & element : elements)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ", ";
            formatElement(element, format);
        }
    }

    void formatSettings(
        const ASTPtr & settings, const IAST::FormatSettings & format)
    {
        if (!settings)
            return;

        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " SETTINGS " << (format.hilite ? IAST::hilite_none : "");

        settings->format(format);
    }
}


void ASTBackupQuery::Element::setCurrentDatabase(const String & current_database)
{
    if (current_database.empty())
        return;

    if (database_name.empty())
        database_name = current_database;
    if (new_database_name.empty())
        new_database_name = current_database;
}


void ASTBackupQuery::setCurrentDatabase(ASTBackupQuery::Elements & elements, const String & current_database)
{
    for (auto & element : elements)
        element.setCurrentDatabase(current_database);
}


String ASTBackupQuery::getID(char) const
{
    return (kind == Kind::BACKUP) ? "BackupQuery" : "RestoreQuery";
}


ASTPtr ASTBackupQuery::clone() const
{
    auto res = std::make_shared<ASTBackupQuery>(*this);
    res->children.clear();

    if (backup_disk)
        res->set(res->backup_disk, backup_disk->clone());

    if (settings)
        res->settings = settings->clone();

    return res;
}


void ASTBackupQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? "BACKUP " : "RESTORE ")
                << (format.hilite ? hilite_none : "");

    formatElements(elements, format);

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO " : " FROM ")
                << (format.hilite ? hilite_none : "");
    backup_disk->format(format);

    if (settings)
        formatSettings(settings, format);
}

}
