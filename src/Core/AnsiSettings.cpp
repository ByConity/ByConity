/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Core/AnsiSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

namespace DB::ANSI {

void onSettingChanged(Settings *s)
{
    bool ansi = static_cast<DialectType>(s->dialect_type) == DialectType::ANSI || static_cast<DialectType>(s->dialect_type) == DialectType::MYSQL;

    // optimizer settings
    s->enable_optimizer = ansi;

    // community settings
    if (ansi) // TODO: need be refactor by https://meego.feishu.cn/clickhousech/story/detail/3001220613
        s->join_use_nulls = ansi;
    s->cast_keep_nullable = ansi;
    s->union_default_mode = ansi ? "DISTINCT" : "";
    s->intersect_default_mode = ansi ? SetOperationMode::DISTINCT : SetOperationMode::ALL;
    s->except_default_mode = ansi ? SetOperationMode::DISTINCT : SetOperationMode::ALL;
    s->prefer_column_name_to_alias = ansi;
    s->data_type_default_nullable = ansi;
    s->decimal_division_use_extended_scale = ansi;
    s->decimal_arithmetic_promote_storage = ansi;
    s->allow_extended_type_conversion = ansi;
    s->parse_literal_as_decimal = ansi;
    s->check_date_overflow = ansi;
}

}
