#include <cstddef>
#include <memory>
#include <common/logger_useful.h>
#include <common/types.h>
#include <Common/Exception.h>
#include <Common/ChineseTokenExtractor.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>

#pragma clang diagnostic push
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
#include <cppjieba/Jieba.hpp>
#pragma clang diagnostic pop

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

std::map<String, ChineseTokenizerPtr> tokenizers;

void ChineseTokenizerFactory::registeChineseTokneizer(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;

    String dict_path;
    String hmm_model_path;
    String user_dict_path;
    String idf_path;
    String stop_words_path;

    config.keys(config_prefix, keys);

    for (const auto & tokenizer_name : keys)
    {
        auto tokneizer_config_prefix = config_prefix + "." + tokenizer_name;

        dict_path = config.getString(tokneizer_config_prefix + ".dict_path", "");
        hmm_model_path = config.getString(tokneizer_config_prefix + ".hmm_model_path", "");
        user_dict_path = config.getString(tokneizer_config_prefix + ".user_dict_path", "");
        idf_path = config.getString(tokneizer_config_prefix + ".idf_path", "");
        stop_words_path = config.getString(tokneizer_config_prefix + ".stop_words_path", "");
        
        tokenizers[tokenizer_name] = std::make_shared<ChineseTokenizer>(
            dict_path, hmm_model_path, user_dict_path, idf_path, stop_words_path);

        LOG_TRACE(getLogger(__func__), "registe chinese tokenizer config name: {} ", tokenizer_name);
    }
}

ChineseTokenizerPtr ChineseTokenizerFactory::tryGetTokenizer(String name)
{
    if( tokenizers.find(name) == tokenizers.end() )
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Can not find chinese tokneizer with name : {} ", name);
    }
    return tokenizers[name];
}

ChineseTokenExtractor::ChineseTokenExtractor(const String & name)
{
    tokenizer = ChineseTokenizerFactory::tryGetTokenizer(name);
}

ChineseTokenExtractor::ChineseTokenExtractor(ChineseTokenizerPtr tokenizer_) : tokenizer(std::move(tokenizer_))
{
}

void ChineseTokenExtractor::preCutString(const String & data,  WordRangesWithIterator & iterator) const
{   
    iterator.word_ranges.clear();
    tokenizer->cutForSearchWithStringRange(data, iterator.word_ranges);
    iterator.iterator = iterator.word_ranges.begin();
}

bool ChineseTokenExtractor::nextInCutString(size_t & token_start, size_t & token_length, WordRangesWithIterator & iterator)
{   
    if (iterator.iterator == iterator.word_ranges.end())
        return false;

    token_start = iterator.iterator->offset;
    token_length = iterator.iterator->len;
    iterator.iterator++;

    return true;
}

void ChineseTokenExtractor::clearForNextCut(WordRangesWithIterator & iterator)
{
    iterator.word_ranges.clear();
    iterator.iterator = iterator.word_ranges.begin();
}

void ChineseTokenExtractor::stringToGinFilter(const String & data, ChineseTokenExtractorPtr token_extractor, GinFilter & gin_filter)
{
    gin_filter.setQueryString(data.data(), data.size());

    size_t token_start = 0;
    size_t token_length = 0;
    WordRangesWithIterator iterator;

    token_extractor->preCutString(data, iterator);

    while (token_extractor->nextInCutString(token_start, token_length,iterator))
    {
        gin_filter.addTerm(data.data() + token_start, token_length);
    }

    token_extractor->clearForNextCut(iterator);
}

void ChineseTokenExtractor::stringPaddedToGinFilter(const String & data, ChineseTokenExtractorPtr token_extractor, GinFilter & gin_filter)
{
    stringToGinFilter(data, token_extractor, gin_filter);
}

void ChineseTokenExtractor::stringLikeToGinFilter(const String & data, ChineseTokenExtractorPtr token_extractor, GinFilter & gin_filter)
{   
    gin_filter.setQueryString(data.data(), data.size());

    size_t begin_offset = 0;
    size_t end_size = data.size();

    if (data.starts_with("%"))
    {
        begin_offset++;
        end_size--;
    }
    if (data.ends_with("%"))
    {
        end_size--;
    }

    stringToGinFilter(data.substr(begin_offset, end_size), token_extractor, gin_filter);
}

}
