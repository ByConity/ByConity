#include <Columns/Collator.h>
#include <Parsers/ASTClusterByElement.h>
#include <IO/Operators.h>


namespace DB
{

void ASTClusterByElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    getColumns()->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "")
        << " INTO "
        << (settings.hilite ? hilite_none : "");
    getTotalBucketNumber()->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "")
        << " BUCKETS"
        << (settings.hilite ? hilite_none : "");
    if (split_number > 0)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " SPLIT_NUMBER " << split_number << (settings.hilite ? hilite_none : "");
    }
    if (is_with_range)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
        << " WITH_RANGE"
        << (settings.hilite ? hilite_none : "");
    }
}

ASTPtr ASTClusterByElement::clone() const
{
    auto clone = std::make_shared<ASTClusterByElement>(*this);
    clone->cloneChildren();
    return clone;
}


}
