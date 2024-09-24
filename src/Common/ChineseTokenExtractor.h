#pragma once

#include <cstddef>
#include <memory>

#include <Interpreters/ITokenExtractor.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/types.h>
#include <Interpreters/GinFilter.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wcast-qual"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#pragma clang diagnostic ignored "-Wsuggest-override"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunknown-pragmas"
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#include <cppjieba/Jieba.hpp>
#pragma clang diagnostic pop

#include <cppjieba/QuerySegment.hpp>


namespace DB
{

using ChineseTokenizer = cppjieba::Jieba;
using ChineseTokenizerPtr = std::shared_ptr<ChineseTokenizer>;
class ChineseTokenExtractor;
using ChineseTokenExtractorPtr = const ChineseTokenExtractor *; 

constexpr auto CHINESE_TOKENIZER_CONFIG_PREFIX = "chinese_tokenizer";

class ChineseTokenizerFactory
{
public:
    static void registeChineseTokneizer(
        const Poco::Util::AbstractConfiguration & config, const String & config_prefix = CHINESE_TOKENIZER_CONFIG_PREFIX);

    static ChineseTokenizerPtr tryGetTokenizer(String name);

};

class ChineseTokenExtractor
{
public:
    using WordRange = cppjieba::WordRangeWithOffset;
    using WordRanges = std::vector<WordRange>;

    struct WordRangesWithIterator
    {
        WordRanges word_ranges;
        WordRanges::iterator iterator;
    };

    explicit ChineseTokenExtractor(const String & name = "default");
    explicit ChineseTokenExtractor(ChineseTokenizerPtr tokenizer_);

    static String getName() { return "token_chinese_default"; }

    void preCutString(const String & data, WordRangesWithIterator & iterator) const;

    static bool nextInCutString(size_t & token_start, size_t & token_length, WordRangesWithIterator & iterator);

    static void clearForNextCut(WordRangesWithIterator & iterator);

    // if we have other nlp tokenizer, refactor function here;
    static void stringToGinFilter(const String & data, ChineseTokenExtractorPtr token_extractor, GinFilter & gin_filter);
    static void stringPaddedToGinFilter(const String & data, ChineseTokenExtractorPtr token_extractor, GinFilter & gin_filter);
    static void stringLikeToGinFilter(const String & data, ChineseTokenExtractorPtr token_extractor, GinFilter & gin_filter);

private:
    ChineseTokenizerPtr tokenizer;
};
}
