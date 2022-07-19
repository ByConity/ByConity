#include <Analyzers/ScopeAwareEquals.h>

namespace DB::ASTEquality
{

std::optional<bool> ScopeAwareEquals::equals(const ASTPtr & left, const ASTPtr & right) const
{
    auto left_col_ref = analysis->tryGetColumnReference(left);
    auto right_col_ref = analysis->tryGetColumnReference(right);

    if (left_col_ref || right_col_ref)
    {
        if (left_col_ref && right_col_ref)
        {
            return left_col_ref->scope == right_col_ref->scope && left_col_ref->local_index == right_col_ref->local_index;
        }

        return false;
    }

    return std::nullopt;
}

std::optional<size_t> ScopeAwareHash::hash(const ASTPtr & ast) const
{
    if (auto col_ref = analysis->tryGetColumnReference(ast))
    {
        auto t = static_cast<size_t>(reinterpret_cast<std::uintptr_t>(col_ref->scope));
        return t * 31 + col_ref->local_index;
    }

    return std::nullopt;
}

}
