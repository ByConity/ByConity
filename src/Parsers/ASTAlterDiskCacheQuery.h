#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>

namespace DB
{

struct ASTAlterDiskCacheQuery : public ASTQueryWithTableAndOutput
{
    enum class Action
    {
        PRELOAD,
        DROP
    };

    enum class Type
    {
        PARTS,
        MANIFEST
    };

    String getID(char delim) const override { return "ALTER DISK CACHE " +  (delim + std::to_string(static_cast<int>(action))); }

    ASTType getType() const override { return ASTType::ASTAlterDiskCacheQuery; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTAlterDiskCacheQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    Action action;
    Type type = Type::PARTS;

    // parts disk cache replated info
    ASTPtr partition;

    // manifest disk cache related info
    ASTPtr version;

    bool sync = true;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ALTER DISK CACHE " << (action == ASTAlterDiskCacheQuery::Action::PRELOAD ? "PRELOAD TABLE " : "DROP TABLE ") << (settings.hilite ? hilite_none : "");

        if (!table.empty())
        {
            if (!database.empty())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(database) << (settings.hilite ? hilite_none : "");
                settings.ostr << ".";
            }
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(table) << (settings.hilite ? hilite_none : "");
        }

        if (type == ASTAlterDiskCacheQuery::Type::MANIFEST)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MANIFEST " << (settings.hilite ? hilite_none : "");
            if (version)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "VERSION " << (settings.hilite ? hilite_none : "");
                version->formatImpl(settings, state, frame);
            }
        }

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (sync ? " SYNC " : " ASYNC ") << (settings.hilite ? hilite_none : "");
    }
};

}
