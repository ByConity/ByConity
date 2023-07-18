#include <Parsers/IParser.h>
#include <Parsers/IAST.h>


namespace DB{

class ParserHints
{
public:
    static void parse(IParser::Pos & pos, SqlHints & hints, Expected & expected);
//    static void parserTableHint(IParser::Pos & pos, SqlHints & hints, Expected & expected);
    static bool parseHint(IParser::Pos & pos, SqlHints & hints, Expected & expected);
    static bool parsehintName(IParser::Pos & pos, String & name, Expected & expected);
    static bool parseHintOptions(IParser::Pos & pos, SqlHint & hint, Expected & expected);
    static bool parseJoinLevelOptions(IParser::Pos & pos, SqlHint & hint, Expected & expected);
    static bool parseJoinPair(IParser::Pos & pos, SqlHint & hint, Expected & expected);
    static bool parseOption(IParser::Pos & pos, SqlHint & hint, bool & first, bool & is_kv_option, Expected & expected);
    static bool parseOptionName(IParser::Pos & pos, String & name, Expected & expected);
};
}
