#pragma once

#include <unordered_map>
#include <utility>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST.h>
#include <Interpreters/Aliases.h>
#include <Parsers/ASTIdentifier.h>
#include "Core/Names.h"
#include "QueryPlan/SymbolAllocator.h"

namespace DB
{
class ASTFunction;
struct QualifiedName;
class ASTSelectQuery;

class SubstituteSelectItemToAnyFunction
{
public:
    explicit SubstituteSelectItemToAnyFunction(ContextMutablePtr context_) : context(std::move(context_))
    {
    }

    void visit(ASTPtr & ast);
    void visit(ASTSelectQuery * select_query);

    bool hasAggregate(ASTPtr & ast);

private:
    ContextMutablePtr context;
};

using QualifiedNames = std::vector<QualifiedName>;
using NameAndQualifiedName = std::vector<std::pair<String, QualifiedName>>;
class SubstituteIdentifierToAnyFunction
{
public:
    struct Data
    {
        const QualifiedNames & grouping_qualified_names;
        NameAndQualifiedName & processed_identifier_qualified_names;
        const NameSet & aliases;
        ContextPtr context;
        bool add_alias;
        bool process_grouping;
        NameSet identifier_aliases;
        explicit Data(const QualifiedNames & grouping_qualified_names_
        , NameAndQualifiedName & processed_identifier_qualified_names_
        , const NameSet & aliases_
        , ContextPtr context_
        , bool add_alias_
        , bool process_grouping_)
        : grouping_qualified_names(grouping_qualified_names_)
        , processed_identifier_qualified_names(processed_identifier_qualified_names_)
        , aliases(aliases_)
        , context(context_)
        , add_alias(add_alias_)
        , process_grouping(process_grouping_)
        {
            identifier_aliases = {};
        }
    };

    explicit SubstituteIdentifierToAnyFunction(Data & data)
        : visitor_data(data)
    {}

    void setAddAlias(bool add_alias){ visitor_data.add_alias = add_alias; }
    void visit(ASTPtr & ast)
    {
        visit(ast, visitor_data);
    }

private:
    Data & visitor_data;

    static void visit(ASTPtr & ast, Data & data);
    static void visit(ASTIdentifier &, ASTPtr &, Data &);

    static void visitChildren(IAST * node, Data & data);
};


}
