#include "StringUtils.h"


namespace detail
{

bool startsWith(const std::string & s, const char * prefix, size_t prefix_size)
{
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool endsWith(const std::string & s, const char * suffix, size_t suffix_size)
{
    return s.size() >= suffix_size && 0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

void parseSlowQuery(const std::string& query, size_t & pos)
{
    const std::string whitespace = " \t\n";
    pos = query.find_first_not_of(whitespace, pos);
    if (pos == std::string::npos || query[pos] != '/')
        return;

    pos++;

    size_t length = query.size();
    if (pos >= length || query[pos] != '*')
        return;

    while (pos < length)
    {
        if (pos + 1 < length && query[pos] == '*' && query[pos + 1] == '/')
        {
            pos += 2;
            break;
        }
        pos++;
    }

    // recursively parse the query
    parseSlowQuery(query, pos);
}

}
