#pragma once

#include <memory>
#include <Interpreters/GinFilter.h>
#include <TSQuery/TSQueryExpression.h>
#include <TSQuery/TSQueryParserDriver.h>
#include <roaring.hh>
#include <Common/ChineseTokenExtractor.h>
#include <common/types.h>
#include <Interpreters/ITokenExtractor.h>

namespace DB
{

class TextSearchQueryExpression;
using TextSearchQueryExpressionPtr = std::unique_ptr<TextSearchQueryExpression>;

class TextSearchQueryExpression
{
public:
    enum class QueryExpressionType
    {
        VAL,
        NOT,
        AND,
        OR
    };

    explicit TextSearchQueryExpression(std::unique_ptr<GinFilter> filter_);
    TextSearchQueryExpression(const QueryExpressionType & type_, TextSearchQueryExpressionPtr left_, TextSearchQueryExpressionPtr right_);

    static bool calculate(const TextSearchQueryExpressionPtr & node, const GinFilter & range_filter, PostingsCacheForStore & cache_store, roaring::Roaring & filter_result);

private:

    std::unique_ptr<TextSearchQueryExpression> left;
    std::unique_ptr<TextSearchQueryExpression> right;
    QueryExpressionType type;
    std::unique_ptr<GinFilter> filter;
};

class TextSearchQuery
{
public:
    explicit TextSearchQuery(const String & query_);

    String toString() const;

    TextSearchQueryExpressionPtr toTextSearchQueryExpression(
        const GinFilterParameters & params, TokenExtractorPtr token_extractor, ChineseTokenExtractorPtr nlp_extractor);

private:
    TSQuery::TSQueryASTPtr parse();

    static TextSearchQueryExpressionPtr toTextSearchQueryExpressionImpl(
        TSQuery::TSQueryASTPtr ast_node, const GinFilterParameters & params, std::function<std::unique_ptr<GinFilter>(String)> get_gin_filter);

    TSQuery::TSQueryASTPtr query_ast;
    String query;
    TSQuery::TSQueryParserDriver dirver;
};


}
