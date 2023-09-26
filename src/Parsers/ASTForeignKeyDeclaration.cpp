#include <Core/ColumnsWithTypeAndName.h>
#include <IO/Operators.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTForeignKeyDeclaration::clone() const
{
    auto res = std::make_shared<ASTForeignKeyDeclaration>();

    res->fk_name = fk_name;
    if (column_names)
        res->column_names = column_names->clone();
    res->ref_table_name = ref_table_name;
    if (ref_column_names)
        res->ref_column_names = ref_column_names->clone();

    return res;
}

void ASTForeignKeyDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (fk_name.empty())
        throw Exception("BUG: fk_name is empty", ErrorCodes::INCORRECT_DATA);

    s.ostr << (s.hilite ? hilite_identifier : "") << fk_name << " " << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_keyword : "") << "FOREIGN KEY " << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") << "(";
    column_names->formatImpl(s, state, frame);
    s.ostr << ")" << (s.hilite ? hilite_none : "");

    s.ostr << " REFERENCES " << backQuoteIfNeed(ref_table_name);

    s.ostr << (s.hilite ? hilite_identifier : "") << "(";
    ref_column_names->formatImpl(s, state, frame);
    s.ostr << ")" << (s.hilite ? hilite_none : "");
}

}
