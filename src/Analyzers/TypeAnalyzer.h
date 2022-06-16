#pragma once

#include <Analyzers/ExprAnalyzer.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{
/**
 * AST type analyzer.
 *
 * Analyze and return the type of given expression.
 */
class TypeAnalyzer : boost::noncopyable
{
public:
    // WARNING: this can be slow
    // if you will use the same `input_types` to getType for many times
    // use the following instead
    // ```
    // auto analyzer = TypeAnalyzer::create(context, input_types);
    // for (...) {...; analyzer.getType(expr); ...;}
    // ```
    static DataTypePtr getType(const ConstASTPtr & expr, ContextMutablePtr context, const NamesAndTypes & input_types);

    static TypeAnalyzer create(ContextMutablePtr context, const NamesAndTypes & input_types);
    DataTypePtr getType(const ConstASTPtr & expr) const;

private:
    TypeAnalyzer(ContextMutablePtr context_, Scope && scope_) : context(std::move(context_)), scope(std::move(scope_)) { }

    ContextMutablePtr context;
    Scope scope;
};


}
