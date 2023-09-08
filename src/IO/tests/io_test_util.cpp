#include <IO/tests/io_test_util.h>
#include <random>

namespace DB {

void replaceStr(String& str, const String& old_str, const String& new_str) {
    std::string::size_type pos = 0u;
    while((pos = str.find(old_str, pos)) != std::string::npos){
        str.replace(pos, old_str.length(), new_str);
        pos += new_str.length();
    }
}

String randomString(size_t length) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, sizeof(alphanum) - 2);

    //    srand((unsigned) time(NULL) * getpid());

    String str(length, '\0');
    for (size_t i = 0; i < length; i++)
    {
        str[i] = alphanum[distribution(generator)];
    }
    return str;
}

}