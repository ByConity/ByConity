#pragma once

#include <vector>
#include <Parsers/Lexer.h>


namespace DB
{

/** Parser operates on lazy stream of tokens.
  * It could do lookaheads of any depth.
  */

/** Used as an input for parsers.
  * All whitespace and comment tokens are transparently skipped.
  */

class Context;
class Tokens
{
private:
    std::vector<Token> data;
    std::vector<size_t> significant_tokens; // store the index of all significant tokens
    Lexer lexer;
    const Context *context = nullptr;

public:
    Tokens(const char * begin, const char * end, size_t max_query_size = 0) : lexer(begin, end, max_query_size) {}

    const Token & operator[] (size_t index)
    {
        while (true)
        {
            if (index < significant_tokens.size())
                return data[significant_tokens[index]];

            if (!data.empty() && data.back().isEnd())
                return data.back();

            Token token = lexer.nextToken();

            if (token.isSignificant())
                significant_tokens.emplace_back(data.size());

            if (token.type != TokenType::Whitespace)
                data.emplace_back(token);
        }
    }

    // return the insignificant token just after the i-th significant token
    const Token * getNextInsignificantToken(size_t index)
    {
        if (!(*this)[index].isEnd())
        {
            (*this)[index + 1]; // ensure the insignificant token has been read

            if (significant_tokens[index + 1] == significant_tokens[index] + 1)
                return nullptr;

            return &data[significant_tokens[index] + 1];
        }

        return nullptr;
    }

    const Token & max()
    {
        if (significant_tokens.empty())
            return (*this)[0];
        return data[significant_tokens.back()];
    }

    const Context *getContext() const 
    {
        return this->context;
    }

    void setContext(const Context *s)
    {
        this->context = s;
    }
};


/// To represent position in a token stream.
class TokenIterator
{
private:
    Tokens * tokens;
    size_t index = 0;

public:
    explicit TokenIterator(Tokens & tokens_) : tokens(&tokens_) {}

    const Token & get() { return (*tokens)[index]; }
    const Token * getLastInsignificantToken()
    {
        return tokens->getNextInsignificantToken(index - 1);
    }
    const Token & operator*() { return get(); }
    const Token * operator->() { return &get(); }

    TokenIterator & operator++() { ++index; return *this; }
    TokenIterator & operator--() { --index; return *this; }

    bool operator< (const TokenIterator & rhs) const { return index < rhs.index; }
    bool operator<= (const TokenIterator & rhs) const { return index <= rhs.index; }
    bool operator== (const TokenIterator & rhs) const { return index == rhs.index; }
    bool operator!= (const TokenIterator & rhs) const { return index != rhs.index; }

    bool isValid() { return get().type < TokenType::EndOfStream; }

    /// Rightmost token we had looked.
    const Token & max() { return tokens->max(); }

    const Context *getContext() const { return tokens->getContext();}
};


/// Returns positions of unmatched parentheses.
using UnmatchedParentheses = std::vector<Token>;
UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin);

}
