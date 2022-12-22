#pragma once
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/PlanVisitor.h>

namespace DB
{
class SymbolsExtractor
{
public:
    static std::set<std::string> extract(ConstASTPtr node);
    static std::set<std::string> extract(PlanNodePtr & node);
    static std::set<std::string> extract(std::vector<ConstASTPtr> & nodes);
};

class SymbolVisitor : public ConstASTVisitor<Void, std::set<std::string>>
{
public:
    Void visitNode(const ConstASTPtr &, std::set<std::string> & context) override;
    Void visitASTIdentifier(const ConstASTPtr &, std::set<std::string> & context) override;
};

}
