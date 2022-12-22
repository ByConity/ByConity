#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context.h>
#include <optional>

namespace DB
{

std::optional<Field> tryEvaluateConstantExpression(const ASTPtr & node, ContextPtr context);

}
