#include <Parsers/parseQuery.h>
#include <Parsers/ParserPartToolkitQuery.h>
#include <iostream>
#include <string>

int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Error! No input part writer query." << std::endl;
        return -1;
    }

    std::string query = argv[1];

    const char * begin = query.data();
    const char * end =  query.data() + query.size();

    DB::ParserPartToolkitQuery parser(end);

    auto ast = DB::parseQuery(parser, begin, end, "", 10000, 100);

    return 0;
}
