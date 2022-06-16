#include <Analyzers/TypeAnalyzer.h>
#include <Analyzers/ExprAnalyzer.h>
#include <Analyzers/Analysis.h>

namespace DB
{

DataTypePtr TypeAnalyzer::getType(const ConstASTPtr & expr, ContextMutablePtr context, const NamesAndTypes & input_types)
{
    return TypeAnalyzer::create(context, input_types).getType(expr);
}

TypeAnalyzer TypeAnalyzer::create(ContextMutablePtr context, const NamesAndTypes & input_types)
{
    FieldDescriptions fields;
    for(const auto & input : input_types) {
        FieldDescription field {input.name, input.type};
        fields.emplace_back(field);
    }
    Scope scope(Scope::ScopeType::RELATION, nullptr, true, fields);
    return TypeAnalyzer(context, std::move(scope));
}

DataTypePtr TypeAnalyzer::getType(const ConstASTPtr & expr) const
{
    Analysis analysis;
    return ExprAnalyzer::analyze(expr->clone(), &scope, context, analysis);
}

}
