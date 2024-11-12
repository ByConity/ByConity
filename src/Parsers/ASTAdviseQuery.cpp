#include <Parsers/ASTAdviseQuery.h>


namespace DB
{

const char * ASTAdviseQuery::getTypeString(ASTAdviseQuery::AdvisorType adviseType)
{
    switch (adviseType)
    {
        case AdvisorType::ALL: return "ALL";
        case AdvisorType::ORDER_BY: return "ORDER_BY";
        case AdvisorType::CLUSTER_BY: return "CLUSTER_BY";
        case AdvisorType::DATA_TYPE: return "DATA_TYPE";
        case AdvisorType::MATERIALIZED_VIEW: return "MATERIALIZED_VIEW";
        case AdvisorType::PROJECTION:
            return "PROJECTION";
        case AdvisorType::COLUMN_USAGE:
            return "COLUMN_USAGE";
    }
}

void ASTAdviseQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "ADVISE " << (settings.hilite ? hilite_none : "") << getTypeString(type);
    if (tables)
        settings.ostr << " TABLES " << (settings.hilite ? hilite_none : "") << tables->formatForErrorMessage()
                      << (settings.hilite ? hilite_none : "");
    if (queries_file)
        settings.ostr << " QUERIES " << (settings.hilite ? hilite_none : "") << "'" << queries_file.value() << "'"
                      << (settings.hilite ? hilite_none : "");
    if (separator)
        settings.ostr << " SEPARATOR " << (settings.hilite ? hilite_none : "") << "'" << separator.value() << "'"
                      << (settings.hilite ? hilite_none : "");
    if (output_ddl)
        settings.ostr << " OUTPUT DDL";
    if (optimized_file)
        settings.ostr << " INTO " << (settings.hilite ? hilite_none : "") << "'" << optimized_file.value() << "'"
                      << (settings.hilite ? hilite_none : "");
    settings.ostr << ";";
}


}
