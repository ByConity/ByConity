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
#include <cppjieba/Jieba.hpp>
#pragma clang diagnostic pop
#include <cppjieba/QuerySegment.hpp>
#include <limonp/StringUtil.hpp>
#include <iostream>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

using namespace std;

int main(int argc, char ** argv)
{   
    if (argc != 7)
    {
        cout << "./chinese_tokenizer [dict_path] [hmm_model_path] [user_doct_path] [idf_path] [stop_words_path] [text_for_cut]" << endl;
        return 0;
    }

    string dict_path(argv[1]);
    string hmm_model_path(argv[2]);
    string user_dict_path(argv[3]);
    string idf_path(argv[4]);
    string stop_words_path(argv[5]);

    string string_for_cut(argv[6]);

    cppjieba::Jieba jieba(dict_path,hmm_model_path, user_dict_path, idf_path, stop_words_path);

    vector<cppjieba::WordRangeWithOffset> words_with_offset;

    jieba.cutForSearchWithStringRange(string_for_cut, words_with_offset);

    for(auto & i : words_with_offset)
    {   
        cout << string_for_cut.substr(i.offset, i.len) << " | ";
    }

    cout << endl;

    return 0;
}
