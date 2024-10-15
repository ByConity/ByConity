/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <cstddef>
#include <Parsers/formatAST.h>


namespace DB
{

void formatAST(const IAST & ast, WriteBuffer & buf, bool hilite, bool one_line, bool always_quote_identifiers, DialectType dialect, bool remove_tenant_id)
{
    IAST::FormatSettings settings(buf, one_line);
    settings.hilite = hilite;
    settings.always_quote_identifiers = always_quote_identifiers;
    settings.dialect_type = dialect;
    settings.remove_tenant_id = remove_tenant_id;

    ast.format(settings);
}

String serializeAST(const IAST & ast, bool one_line, bool always_quote_identifiers)
{
    WriteBufferFromOwnString buf;
    formatAST(ast, buf, false, one_line, always_quote_identifiers);
    return buf.str();
}

String serializeASTWithOutAlias(const IAST & ast)
{
    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(buf, true);
    settings.hilite = false;
    settings.without_alias = true;
    ast.format(settings);
    return buf.str();
}

String getSerializedASTWithLimit(const IAST & ast, size_t max_text_length)
{
    String res = serializeAST(ast);
    if (res.size() <= max_text_length)
        return res;
    else
        return res.substr(0, max_text_length);
}

}
