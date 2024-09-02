#pragma once

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTimeInterval.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

enum class RefreshScheduleKind : UInt8
{
    UNKNOWN = 0,
    ASYNC,  /// refresh task execute in async mode as backgroud task
    SYNC,   /// automatic trigger when source table insert data (normal mode)
    MANUAL  /// refresh mv in manual mode (not automatic tigger)
};

String toString(RefreshScheduleKind refresh_kind);

inline constexpr RefreshScheduleKind getRefreshScheduleKind(std::string_view kind_name)
{
    RefreshScheduleKind refresh_kind = RefreshScheduleKind::UNKNOWN;
    if (kind_name == "UNKNOWN")
        refresh_kind = RefreshScheduleKind::UNKNOWN;
    else if (kind_name == "ASYNC")
        refresh_kind = RefreshScheduleKind::ASYNC;
    else if (kind_name == "SYNC")
        refresh_kind = RefreshScheduleKind::SYNC;
    else if (kind_name == "MANUAL")
        refresh_kind = RefreshScheduleKind::MANUAL;
    return refresh_kind;
}

/// Strategy for MATERIALIZED VIEW ... REFRESH ..
class ASTRefreshStrategy : public IAST
{
public:
    RefreshScheduleKind schedule_kind{RefreshScheduleKind::UNKNOWN};
    ASTLiteral * start = nullptr;
    ASTTimeInterval * time_interval = nullptr;

    String getID(char) const override { return "Refresh strategy definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
