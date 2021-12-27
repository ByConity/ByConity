#include <Columns/Collator.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTOrderByElement::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(direction);
    hash_state.update(nulls_direction);
    hash_state.update(nulls_direction_was_explicitly_specified);
    hash_state.update(with_fill);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTOrderByElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "")
        << (direction == -1 ? " DESC" : " ASC")
        << (settings.hilite ? hilite_none : "");

    if (nulls_direction_was_explicitly_specified)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << " NULLS "
            << (nulls_direction == direction ? "LAST" : "FIRST")
            << (settings.hilite ? hilite_none : "");
    }

    if (collation)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " COLLATE " << (settings.hilite ? hilite_none : "");
        collation->formatImpl(settings, state, frame);
    }

    if (with_fill)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH FILL" << (settings.hilite ? hilite_none : "");
        if (fill_from)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
            fill_from->formatImpl(settings, state, frame);
        }
        if (fill_to)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "");
            fill_to->formatImpl(settings, state, frame);
        }
        if (fill_step)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " STEP " << (settings.hilite ? hilite_none : "");
            fill_step->formatImpl(settings, state, frame);
        }
    }
}

void ASTOrderByElement::serialize(WriteBuffer & buf) const
{
    writeBinary(direction, buf);
    writeBinary(nulls_direction, buf);
    writeBinary(nulls_direction_was_explicitly_specified, buf);

    serializeAST(collation, buf);

    writeBinary(with_fill, buf);
    serializeAST(fill_from, buf);
    serializeAST(fill_to, buf);
    serializeAST(fill_step, buf);

    serializeASTs(children, buf);
}

void ASTOrderByElement::deserializeImpl(ReadBuffer & buf)
{
    readBinary(direction, buf);
    readBinary(nulls_direction, buf);
    readBinary(nulls_direction_was_explicitly_specified, buf);

    collation = deserializeAST(buf);

    readBinary(with_fill, buf);
    fill_from = deserializeAST(buf);
    fill_to = deserializeAST(buf);
    fill_step = deserializeAST(buf);

    children = deserializeASTs(buf);
}

ASTPtr ASTOrderByElement::deserialize(ReadBuffer & buf)
{
    auto orde_by_element = std::make_shared<ASTOrderByElement>();
    orde_by_element->deserializeImpl(buf);
    return orde_by_element;
}

}
