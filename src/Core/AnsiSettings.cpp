#include <Core/AnsiSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

namespace DB::ANSI {

void onSettingChanged(Settings *s)
{
    if (!s->dialect_type.pending)
        return;

    s->dialect_type.pending = false;

    bool ansi = static_cast<DialectType>(s->dialect_type) == DialectType::ANSI;

    // optimizer settings
    s->enable_optimizer = true;

    // community settings
    s->join_use_nulls = ansi;
    s->cast_keep_nullable = ansi;
    s->union_default_mode = ansi ? "DISTINCT" : "";
    s->intersect_default_mode = ansi ? UnionMode::DISTINCT : UnionMode::ALL;
    s->except_default_mode = ansi ? UnionMode::DISTINCT : UnionMode::ALL;
    s->allow_experimental_window_functions = ansi;
    s->prefer_column_name_to_alias = ansi;
    s->data_type_default_nullable = ansi;
    s->enable_replace_group_by_literal_to_symbol = ansi;
    s->enable_replace_order_by_literal_to_symbol = ansi;
}

}
