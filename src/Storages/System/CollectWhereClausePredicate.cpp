#include <Storages/System/CollectWhereClausePredicate.h>

namespace DB
{
    // collects columns and values for WHERE condition with AND and EQUALS (case insesitive)
    // e.g for query "select ... where db = 'x' ", method will return {'db' : 'x'} map
    void collectWhereClausePredicate(const ASTPtr & ast, std::map<String,String> & columnToValue)
    {
        static String columnName;
        if (!ast)
            return;

        if (ASTFunction * func = ast->as<ASTFunction>())
        {
            if (func->name == "equals" || func->name == "and")
            {
                for (auto & arg : func->arguments->children)
                {
                    collectWhereClausePredicate(arg, columnToValue); // recurse in a depth first fashion
                }
            }
        }
        else if (ASTIdentifier * identifier = ast->as<ASTIdentifier>())
        {
            columnName = identifier->name();
        }
        else if (ASTLiteral * literal = ast->as<ASTLiteral>())
        {
            if (literal->value.getType() == Field::Types::String)
            {
                columnToValue.emplace(columnName,literal->value.get<String>());
            }
        }
    }


    std::pair<String,String> collectWhereEqualClausePredicate(const ASTPtr & ast)
    {
        std::pair<String,String> res;
        if (!ast)
            return res;

        ASTFunction * func = ast->as<ASTFunction>();
        if (!func)
            return res;

        if (func->name != "equals")
            return res;

        const auto * column_name = func->arguments->children[0]->as<ASTIdentifier>();
        const auto * column_value = func->arguments->children[1]->as<ASTLiteral>();

        if (!column_name || !column_value)
            return res;

        if (column_value->value.getType() != Field::Types::String)
            return std::make_pair(column_name->name(), "");

        return std::make_pair(column_name->name(), column_value->value.get<String>());
    }

    std::map<String,String> collectWhereANDClausePredicate(const ASTPtr & ast)
    {
        std::map<String,String> res;
        if (!ast)
            return res;

        ASTFunction * func = ast->as<ASTFunction>();
        if (!func)
            return res;

        else if ((func->name == "and") && func->arguments)
        {
            const auto children = func->arguments->children;
            std::for_each(children.begin(), children.end(), [& res] (const auto & child) {
                std::pair<String, String> p = collectWhereEqualClausePredicate(child);
                if (!p.first.empty())
                    res.insert(p);
            });
        }
        else if (func->name == "equals")
        {
            auto p = collectWhereEqualClausePredicate(ast);
            if (!p.first.empty())
                res.insert(p);
        }

        return res;
    }

    // collects columns and values for WHERE condition with OR, AND and EQUALS (case insesitive)
    // e.g for query "select ... where ((db = 'db') AND (name = 'name1')) OR ((db = 'db') AND (name = 'name2')) ", method will return a vector {'name':'name1', 'db':'db'}, {'db' : 'db', 'name':'name2'}
    // if a value of column is not a string, it will has value as an empty string
    std::vector<std::map<String,String>> collectWhereORClausePredicate(const ASTPtr & ast)
    {
        std::vector<std::map<String,String>> res;
        if (!ast)
            return res;

        ASTFunction * func = ast->as<ASTFunction>();
        if (!func)
            return res;

        if ((func->name == "or") && func->arguments)
        {
            const auto children = func->arguments->children;
            std::for_each(children.begin(), children.end(), [& res] (const auto & child) {
                std::map<String,String> m = collectWhereANDClausePredicate(child);
                if (!m.empty())
                    res.push_back(m);
            });
        }
        else if (func->name == "and")
        {
            std::map<String,String> m = collectWhereANDClausePredicate(ast);
            if (!m.empty())
                res.push_back(m);
        }
        else if (func->name == "equals")
        {
            auto p = collectWhereEqualClausePredicate(ast);
            if (!p.first.empty())
                res.push_back(std::map<String, String>{p});
        }

        return res;
    }
}
