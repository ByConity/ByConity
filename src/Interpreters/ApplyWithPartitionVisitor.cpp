#include <Interpreters/ApplyWithPartitionVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
void ApplyWithPartitionVisitor::visit(ASTPtr & ast, const Data & data)
{
    for (auto & child : ast->children)
        visit(child, data);
    if (auto * node_func = ast->as<ASTFunction>())
        visit(*node_func, ast, data);
}

void ApplyWithPartitionVisitor::visit(ASTFunction & func, ASTPtr & node, const Data & data)
{
    auto name = func.getColumnName();
    if (data.partition_sample_block.has(name))
    {
        ASTPtr new_ast = std::make_shared<ASTIdentifier>(name);
        node = std::move(new_ast);
    }
}
}
