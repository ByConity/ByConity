#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>

namespace DB
{

struct ASTAlterDiskCacheQuery : public ASTQueryWithTableAndOutput
{
    enum class Type
    {
        PRELOAD,
        DROP
    };

    String getID(char delim) const override { return "ALTER DISK CACHE " +  (delim + std::to_string(static_cast<int>(type))); }

    ASTType getType() const override { return ASTType::ASTAlterDiskCacheQuery; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTAlterDiskCacheQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    Type type;
    ASTPtr partition;
    bool sync = true;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ALTER DISK CACHE " << (type == ASTAlterDiskCacheQuery::Type::PRELOAD ? "PRELOAD TABLE " : "DROP TABLE ") << (settings.hilite ? hilite_none : "");

        if (!table.empty())
        {
            if (!database.empty())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(database) << (settings.hilite ? hilite_none : "");
                settings.ostr << ".";
            }
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(table) << (settings.hilite ? hilite_none : "");
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (sync ? " SYNC " : " ASYNC ") << (settings.hilite ? hilite_none : "");

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
};

}
