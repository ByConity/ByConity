#include <Common/config.h>
#if USE_TSQUERY

#include <cstddef>
#include <memory>
#include <utility>
#include "common/logger_useful.h"
#include <Common/ChineseTokenExtractor.h>
#include <Common/Exception.h>
#include <common/types.h>
#include <Common/TextSreachQuery.h>
#include <Interpreters/ITokenExtractor.h>
#include <TSQuery/TSQueryException.h>
#include <Interpreters/GinFilter.h>
#include <roaring.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

TextSearchQueryExpression::TextSearchQueryExpression(std::unique_ptr<GinFilter> filter_)
    : left(nullptr), right(nullptr), type(QueryExpressionType::VAL), filter(std::move(filter_))
{
}

TextSearchQueryExpression::TextSearchQueryExpression(
    const QueryExpressionType & type_, TextSearchQueryExpressionPtr left_, TextSearchQueryExpressionPtr right_)
    : left(std::move(left_)), right(std::move(right_)), type(type_), filter(nullptr)
{
}

// calculate return is false, the filter_result(delete bitmap) is empty, 
// maybe we need flip first in some calculate.
bool TextSearchQueryExpression::calculate(
    const TextSearchQueryExpressionPtr & node,
    const GinFilter & range_filter,
    PostingsCacheForStore & cache_store,
    roaring::Roaring & filter_result)
{
    if (node == nullptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad expression with query ast");
    }

    switch (node->type)
    {
        case QueryExpressionType::VAL: {
            //We assume that, there only one term per filter in text search;
            String token = *(node->filter->getTerms().begin());
            node->filter->setQueryString(token.data(), token.size());
            range_filter.contains(*(node->filter), cache_store, &filter_result);

            // maybe we need add some settings here when not found.
            return !filter_result.isEmpty();
        }
        case QueryExpressionType::NOT: {
            roaring::Roaring left_result;
            
            calculate(node->left, range_filter, cache_store, left_result);

            range_filter.filpWithRange(left_result);
            
            filter_result = left_result;
            return !filter_result.isEmpty();
        }
        case QueryExpressionType::AND: {
            roaring::Roaring left_result;
            roaring::Roaring right_result;

            calculate(node->left, range_filter, cache_store, left_result);
            calculate(node->right, range_filter, cache_store, right_result);

            filter_result = left_result & right_result;

            return !filter_result.isEmpty();
        }
        case QueryExpressionType::OR: {
            roaring::Roaring left_result;
            roaring::Roaring right_result;

            calculate(node->left, range_filter, cache_store, left_result);
            calculate(node->right, range_filter, cache_store, right_result);

            filter_result = left_result | right_result;
            return !filter_result.isEmpty();
        }
    }
}

TextSearchQuery::TextSearchQuery(const String & query_):query(query_),dirver()
{
   query_ast = parse();
}

TSQuery::TSQueryASTPtr TextSearchQuery::parse()
{   
    return dirver.parse(query);
}

String TextSearchQuery::toString() const
{
    if( query_ast == nullptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error format with full text search query");
    }

    return query_ast->toString();
}

TextSearchQueryExpressionPtr TextSearchQuery::toTextSearchQueryExpressionImpl(
    TSQuery::TSQueryASTPtr ast_node, const GinFilterParameters & params, std::function<std::unique_ptr<GinFilter>(String)> get_gin_filter)
{
    if (ast_node == nullptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad argument with query ast");
    }

    if (ast_node->getQueryItermType() == TSQuery::TSQueryAST::QueryItermType::QI_VAL)
    {
        auto gin_filter = get_gin_filter(ast_node->getQueryOperand().val);
        return std::make_unique<TextSearchQueryExpression>(std::move(gin_filter));
    }
    else
    {
        switch (ast_node->getQueryOperator().type)
        {
            case TSQuery::TSQueryAST::QueryOperatorType::OP_OR: {
                auto left_node = toTextSearchQueryExpressionImpl(ast_node->getLeftNode(), params, get_gin_filter);
                auto right_node = toTextSearchQueryExpressionImpl(ast_node->getRightNode(), params, get_gin_filter);

                return std::make_unique<TextSearchQueryExpression>(
                    TextSearchQueryExpression::QueryExpressionType::OR, std::move(left_node), std::move(right_node));
            }
            case TSQuery::TSQueryAST::QueryOperatorType::OP_AND: {
                auto left_node = toTextSearchQueryExpressionImpl(ast_node->getLeftNode(), params, get_gin_filter);
                auto right_node = toTextSearchQueryExpressionImpl(ast_node->getRightNode(), params, get_gin_filter);

                return std::make_unique<TextSearchQueryExpression>(
                    TextSearchQueryExpression::QueryExpressionType::AND, std::move(left_node), std::move(right_node));
            }
            case TSQuery::TSQueryAST::QueryOperatorType::OP_NOT: {
                auto left_node = toTextSearchQueryExpressionImpl(ast_node->getLeftNode(), params, get_gin_filter);

                return std::make_unique<TextSearchQueryExpression>(
                    TextSearchQueryExpression::QueryExpressionType::NOT, std::move(left_node), nullptr);
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unkonwn query operator type with text search query.");
        }
    }
}

TextSearchQueryExpressionPtr TextSearchQuery::toTextSearchQueryExpression(
    const GinFilterParameters & params, TokenExtractorPtr token_extractor, ChineseTokenExtractorPtr nlp_extractor)
{
    if(token_extractor != nullptr)
    {
        auto get_gin_filter = [token_extractor, &params](String token) -> std::unique_ptr<GinFilter>
        {
            auto gin_filter = std::make_unique<GinFilter>(params);
            ITokenExtractor::stringToGinFilter(token.data(), token.size(), token_extractor, *gin_filter);
            return gin_filter;
        } ;
        return toTextSearchQueryExpressionImpl(query_ast, params, get_gin_filter);
    }
    else
    {
        auto get_gin_filter = [nlp_extractor, &params](String token) -> std::unique_ptr<GinFilter>
        {
            auto gin_filter = std::make_unique<GinFilter>(params);
            ChineseTokenExtractor::stringToGinFilter(token, nlp_extractor, *gin_filter);
            return gin_filter;
        } ;   
        return toTextSearchQueryExpressionImpl(query_ast, params, get_gin_filter);
    }
}

}

#endif
