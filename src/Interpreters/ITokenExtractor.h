#pragma once

#include <common/types.h>
#include <Interpreters/GinFilter.h>

namespace DB
{

struct ITokenExtractor;
using TokenExtractorPtr = const ITokenExtractor *;

/// Interface for string parsers.
struct ITokenExtractor
{
    virtual ~ITokenExtractor() = default;

    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of extractor).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool nextInString(
        const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
        = 0;

    /// Optimized version that can assume at least 15 padding bytes after data + len (as our Columns provide).
    virtual bool nextInStringPadded(
        const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
    {
        return nextInString(data, length, pos, token_start, token_length);
    }

    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool nextInStringLike(const char * data, size_t length, size_t * pos, String & out) const = 0;


    static void stringToGinFilter(const char * data, size_t length, TokenExtractorPtr token_extractor, GinFilter & gin_filter)
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;


        while (cur < length && token_extractor->nextInString(data, length, &cur, &token_start, &token_len))
            gin_filter.addTerm(data + token_start, token_len);
    }

    static void stringPaddedToGinFilter(const char * data, size_t length, TokenExtractorPtr token_extractor, GinFilter & gin_filter)
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && token_extractor->nextInStringPadded(data, length, &cur, &token_start, &token_len))
            gin_filter.addTerm(data + token_start, token_len);
    }

    static void stringLikeToGinFilter(const char * data, size_t length, TokenExtractorPtr token_extractor, GinFilter & gin_filter)
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        String token;

        gin_filter.setQueryString(data, length);

        while (cur < length && token_extractor->nextInStringLike(data, length, &cur, token))
            gin_filter.addTerm(token.c_str(), token.size());
    }
};


/// Parser extracting all ngrams from string.
struct NgramTokenExtractor final : public ITokenExtractor
{
    explicit NgramTokenExtractor(size_t n_) : n(n_) {}

    static const char * getName() { return "ngrambf_v1"; }

    bool nextInString(const char * data, size_t length, size_t *  __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    size_t getN() const { return n; }

private:

    size_t n;
};

/// Parser extracting tokens (sequences of numbers and ascii letters).
struct SplitTokenExtractor final : public ITokenExtractor
{
    static const char * getName() { return "tokenbf_v1"; }

    bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;

    bool nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t * __restrict pos, String & token) const override;

};

class StandardTokenExtractor final : public ITokenExtractor
{
public:
    static const char * getName() { return "standard"; }

    bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & out) const override;
};

}




