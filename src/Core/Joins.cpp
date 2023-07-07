#include <Core/Joins.h>

namespace DB
{

const char * toString(JoinKind kind)
{
    switch (kind)
    {
        case JoinKind::Inner: return "INNER";
        case JoinKind::Left: return "LEFT";
        case JoinKind::Right: return "RIGHT";
        case JoinKind::Full: return "FULL";
        case JoinKind::Cross: return "CROSS";
        case JoinKind::Comma: return "COMMA";
    }
};

const char * toString(JoinLocality locality)
{
    switch (locality)
    {
        case JoinLocality::Unspecified: return "UNSPECIFIED";
        case JoinLocality::Local: return "LOCAL";
        case JoinLocality::Global: return "GLOBAL";
    }
}

const char * toString(ASOFJoinInequality asof_join_inequality)
{
    switch (asof_join_inequality)
    {
        case ASOFJoinInequality::None: return "NONE";
        case ASOFJoinInequality::Less: return "LESS";
        case ASOFJoinInequality::Greater: return "GREATER";
        case ASOFJoinInequality::LessOrEquals: return "LESS_OR_EQUALS";
        case ASOFJoinInequality::GreaterOrEquals: return "GREATER_OR_EQUALS";
    }
}

const char * toString(JoinTableSide join_table_side)
{
    switch (join_table_side)
    {
        case JoinTableSide::Left: return "LEFT";
        case JoinTableSide::Right: return "RIGHT";
    }
}

}
