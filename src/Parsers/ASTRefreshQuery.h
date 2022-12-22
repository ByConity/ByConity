#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>

namespace DB
{
    class ASTRefreshQuery : public ASTQueryWithTableAndOutput
    {
    public:
        ASTPtr partition; // partition to refresh
        bool async = false;

        String getID(char delimiter) const override
        {
            return "RefreshQuery" + (delimiter + database) + delimiter + table;
        }

        ASTType getType() const override { return ASTType::ASTRefreshQuery; }

        ASTPtr clone() const override
        {
            auto res = std::make_shared<ASTRefreshQuery>(*this);
            res->children.clear();

            if (partition)
            {
                res->partition = partition->clone();
                res->children.push_back(res->partition);
            }
            cloneOutputOptions(*res);

            return res;
        }

    protected:
        void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
        {
            frame.need_parens = false;
            std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "REFRESH MATERIALIZED VIEW " << (settings.hilite ? hilite_none : "");

            if (!table.empty())
            {
                if (!database.empty())
                {
                    settings.ostr << indent_str << backQuoteIfNeed(database);
                    settings.ostr << ".";
                }
                settings.ostr << indent_str << backQuoteIfNeed(table);
            }

            if (partition)
            {
                settings.ostr << settings.nl_or_ws << (settings.hilite ? hilite_keyword : "") << indent_str << "    PARTITION "
                              << (settings.hilite ? hilite_none : "");

                FormatStateStacked frame_nested = frame;
                frame_nested.need_parens = false;
                ++frame_nested.indent;
                partition->formatImpl(settings, state, frame_nested);
            }

            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (async ? " ASYNC" : " SYNC")
                          << (settings.hilite ? hilite_none : "") << settings.nl_or_ws;
        }
    };
}

