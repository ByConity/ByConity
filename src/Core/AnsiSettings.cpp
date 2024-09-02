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
    bool ansi_like = static_cast<DialectType>(s->dialect_type) != DialectType::CLICKHOUSE;
    bool mysql = static_cast<DialectType>(s->dialect_type) == DialectType::MYSQL;

    // optimizer settings
    s->enable_optimizer = ansi_like;

    // community settings
    if (ansi_like) // TODO: need be refactor by https://meego.feishu.cn/clickhousech/story/detail/3001220613
        s->join_use_nulls = ansi_like;
    s->cast_keep_nullable = ansi_like;
    s->union_default_mode = ansi_like ? "DISTINCT" : "";
    s->intersect_default_mode = ansi_like ? SetOperationMode::DISTINCT : SetOperationMode::ALL;
    s->except_default_mode = ansi_like ? SetOperationMode::DISTINCT : SetOperationMode::ALL;
    s->prefer_column_name_to_alias = ansi_like;
    s->data_type_default_nullable = ansi_like;
    s->decimal_division_use_extended_scale = ansi_like;
    s->decimal_arithmetic_promote_storage = ansi_like;
    s->allow_extended_type_conversion = ansi_like;
    s->parse_literal_as_decimal = ansi_like;
    s->check_data_overflow = mysql;
    // s->text_case_option= mysql ? TextCaseOption::LOWERCASE : TextCaseOption::MIXED;
    s->enable_implicit_arg_type_convert = mysql;
    s->handle_division_by_zero = mysql;
    s->exception_on_unsupported_mysql_syntax = !mysql;
}

}
