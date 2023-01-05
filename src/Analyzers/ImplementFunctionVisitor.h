#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

// this visitor keeps same ASTFunction rewriting logic of `TranslateQualifiedNamesVisitor` & `QueryNormalizer`
struct ImplementFunction
{
    using TypeToVisit = ASTFunction;

    struct ExtractedSettings
    {
        const String count_distinct_implementation;

        template <typename T>
        ExtractedSettings(const T & settings_): // NOLINT(google-explicit-constructor)
            count_distinct_implementation(settings_.count_distinct_implementation)
        {}
    };

    explicit ImplementFunction(ContextMutablePtr context_):
        context(std::move(context_)), settings(context->getSettingsRef())
    {}

    void visit(ASTFunction & function, ASTPtr & ast);

    static void rewriteAsTranslateQualifiedNamesVisitorDo(ASTFunction & node);
    static void rewriteAsQueryNormalizerDo(ASTFunction & node);

    ContextMutablePtr context;
    ExtractedSettings settings;
};

using ImplementFunctionMatcher = OneTypeMatcher<ImplementFunction>;
using ImplementFunctionVisitor = InDepthNodeVisitor<ImplementFunctionMatcher, true>;

}
