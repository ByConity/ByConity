#include <Interpreters/Aliases.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

void Aliases::checkSingleton(const String & name) const
{
    if (names_to_asts.count(name) > 1)
        throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS, "Different expressions with the same alias {}", backQuoteIfNeed(name));
}

void Aliases::checkSingleton() const
{
    auto it = names_to_asts.begin(), end = names_to_asts.end();

    while (it != end)
    {
        auto next_it = std::next(it, 1);
        if (next_it != end && it->first == next_it->first)
            throw Exception(
                ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS, "Different expressions with the same alias {}", backQuoteIfNeed(it->first));

        it = next_it;
    }
}

ASTPtr & Aliases::getImpl(const String & name, bool insert_if_not_exists)
{
    checkSingleton(name);

    auto it = names_to_asts.find(name);

    if (it != names_to_asts.end())
        return it->second;
    else if (insert_if_not_exists)
        return names_to_asts.emplace(name, ASTPtr{})->second;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Alias {} not found", backQuoteIfNeed(name));
}
}
