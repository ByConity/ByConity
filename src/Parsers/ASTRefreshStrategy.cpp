#include <Parsers/ASTRefreshStrategy.h>

#include <IO/Operators.h>

namespace DB
{

String toString(RefreshScheduleKind refresh_kind)
{
    switch (refresh_kind)
    {
        case RefreshScheduleKind::UNKNOWN:
            return "UNSPECIFIED";
        case RefreshScheduleKind::ASYNC:
            return "ASYNC";
        case RefreshScheduleKind::SYNC:
            return "SYNC";
        case RefreshScheduleKind::MANUAL:
            return "MANUAL";
    }
}

ASTPtr ASTRefreshStrategy::clone() const
{
    auto res = std::make_shared<ASTRefreshStrategy>(*this);
    res->children.clear();
    res->schedule_kind = schedule_kind;
    if (start)
        res->set(res->start, start->clone());

    if (time_interval)
        res->set(res->time_interval, time_interval->clone());
    return res;
}

void ASTRefreshStrategy::formatImpl(
    const IAST::FormatSettings & f_settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    frame.need_parens = false;
    f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << "REFRESH " << (f_settings.hilite ? hilite_none : "");
    switch (schedule_kind)
    {
        case RefreshScheduleKind::ASYNC:
            f_settings.ostr << "ASYNC " << (f_settings.hilite ? hilite_none : "");
            if (start)
            {
                f_settings.ostr << "START(" << (f_settings.hilite ? hilite_none : "");
                start->formatImpl(f_settings, state, frame);
                f_settings.ostr << ")" << (f_settings.hilite ? hilite_none : "");
            }
            if (time_interval)
            {
                f_settings.ostr << " EVERY(INTERVAL " << (f_settings.hilite ? hilite_none : "");
                time_interval->formatImpl(f_settings, state, frame);
                f_settings.ostr << ")" << (f_settings.hilite ? hilite_none : "");
            }
            break;
        case RefreshScheduleKind::SYNC:
            f_settings.ostr << "SYNC " << (f_settings.hilite ? hilite_none : "");
            break;
        case RefreshScheduleKind::MANUAL:
            f_settings.ostr << "MANUAL " << (f_settings.hilite ? hilite_none : "");
            break;
        default:
            f_settings.ostr << (f_settings.hilite ? hilite_none : "");
            break;
    }
}

}
