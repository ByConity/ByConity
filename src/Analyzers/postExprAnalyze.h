#pragma once

#include <Analyzers/Analysis.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
// postponed analyze used for subcolumn optimization
void postExprAnalyze(
    ASTFunctionPtr & function, const ColumnsWithTypeAndName & processed_arguments, Analysis & analysis, ContextPtr context);
}
