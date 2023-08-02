
#include <Common/quoteString.h>
#include "Functions/UserDefined/UDFFlags.h"
#include <IO/Operators.h>
#include <Parsers/ASTExternalFunction.h>


namespace DB
{

ASTPtr ASTExternalFunction::clone() const
{
    return std::make_shared<ASTExternalFunction>(*this);
}

void ASTExternalFunction::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    UDFLanguage language = getLanguage(args.flags);

    if (args.ret) {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " RETURNS " << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_identifier : "") << args.ret->getName() << (settings.hilite ? hilite_none : "");
    }

    auto z = getUserDefinedFlags(args.flags);
    if (z != SCALAR_UDF_DEFAULT_FLAGS) {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FLAGS " << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_identifier : "") << z << (settings.hilite ? hilite_none : "");
    }

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " LANGUAGE " << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getLanguageStr(language)) << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");

    settings.ostr << "\n$" << tag << "$" << (settings.hilite ? hilite_none : "");
    settings.ostr << body;
    settings.ostr << "$" << tag << "$\n" << (settings.hilite ? hilite_none : "");
}

}
