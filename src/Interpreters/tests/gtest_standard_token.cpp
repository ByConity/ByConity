#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <Interpreters/ITokenExtractor.h>

using namespace DB;


TEST(TokenExtractor, StandardToken)
{
    size_t pos = 0;
    StandardTokenExtractor tokenizer;

    size_t token_start = 0;
    size_t token_length = 0;


    size_t index = 0;
    std::string test_str_1 = "ByConity是分布式的云原生SQL数仓引擎";
    std::vector<std::string> test_token_1 = {"ByConity","是","分","布","式","的","云","原","生","SQL","数","仓","引","擎"};
    while(tokenizer.nextInString(
        test_str_1.data(), test_str_1.length(), &pos, &token_start, &token_length))
    {
        auto tmp_str = std::string(test_str_1.data()+token_start, token_length);
        ASSERT_EQ(tmp_str, test_token_1[index]);
        ++index;
    }

    pos = 0;
    index = 0;
    token_start = 0;
    token_length = 0;
    std::string test_str_2 = "StandardToken:分词器,可以跳过ASCII符号.,/!@#$@()-空格等并整块切分english token,123456789和单个切分中文";
    std::vector<std::string>  test_token_2 = {
        "StandardToken","分","词","器","可","以","跳","过","ASCII",
        "符","号","空","格","等","并","整","块","切","分",
        "english","token","123456789","和","单","个","切","分","中","文"
    };

    while(tokenizer.nextInString(
        test_str_2.data(), test_str_2.length(), &pos, &token_start, &token_length))
    {
        auto tmp_str = std::string(test_str_2.data()+token_start, token_length);
        ASSERT_EQ(tmp_str, test_token_2[index]);
        ++index;
    }
}

TEST(TokenExtractor, StandardTokenLike)
{
    size_t pos = 0;
    StandardTokenExtractor tokenizer;
    std::string tmp_token;
    size_t index = 0;

    pos = 0;
    std::string test_str_1 = "%NOTOKEN%";
    while(tokenizer.nextInStringLike(test_str_1.data(), test_str_1.length(), &pos, tmp_token))
    {
        if(!tmp_token.empty())
        {
            throw std::runtime_error("should no token here");
        }
    }

    pos = 0;
    std::string test_str_2 = "%NOTOKEN";
    while(tokenizer.nextInStringLike(test_str_2.data(), test_str_2.length(), &pos, tmp_token))
    {
        if(!tmp_token.empty())
        {
            throw std::runtime_error("should no token here");
        }
    }

    pos = 0;
    std::string test_str_3 = "NOTOKEN%";
    while(tokenizer.nextInStringLike(test_str_3.data(), test_str_3.length(), &pos, tmp_token))
    {
        if(!tmp_token.empty())
        {
            throw std::runtime_error("should no token here");
        }
    }


    pos = 0;
    std::string test_str_4 = "NO_TOKEN";
    while(tokenizer.nextInStringLike(test_str_4.data(), test_str_4.length(), &pos, tmp_token))
    {
        if(!tmp_token.empty())
        {
            throw std::runtime_error("should no token here");
        }
    }

    index = 0;
    pos = 0;
    std::string test_str_5 = "%这里_会有中文token%";
    std::vector<std::string> test_tokens_5 = {"这","里","会","有","中","文"}; 
    while(tokenizer.nextInStringLike(test_str_5.data(), test_str_5.length(), &pos, tmp_token))
    {
        ASSERT_EQ(tmp_token, test_tokens_5[index]);
        index++;
    }

    index = 0;
    pos = 0;
    std::string test_str_6 = "%这里_,english %Token也有%";
    std::vector<std::string> test_tokens_6 = {"这","里","english","也","有"}; 
    while(tokenizer.nextInStringLike(test_str_6.data(), test_str_6.length(), &pos, tmp_token))
    {
        ASSERT_EQ(tmp_token, test_tokens_6[index]);
        index++;
    }
}
