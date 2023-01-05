#pragma once

#include <Parsers/ASTWindowDefinition.h>

namespace DB
{

struct ResolvedWindow;

using ResolvedWindowPtr = std::shared_ptr<ResolvedWindow>;

struct ResolvedWindow
{
    const ASTWindowDefinition * origin_ast;
    ASTPtr partition_by;
    ASTPtr order_by;
    WindowFrame frame;
};

// Resolve an ASTWindowDefinition to a ResolvedWindow by
//   1. combine inherited properties if a parent window name exists
//   2. evaluate boundary offset values
// See also: `ExpressionAnalyzer::makeWindowDescriptionFromAST`
ResolvedWindowPtr resolveWindow(const ASTPtr & node,
                                const std::unordered_map<String, ResolvedWindowPtr> & registered_windows,
                                ContextPtr context);
}
