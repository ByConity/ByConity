#include <Core/Names.h>

namespace DB
{

const static std::unordered_set<char> BITENGINE_EXPRESSION_KEYWORDS = {'&', '|', '~', ',', '#', ' ', '(', ')'};
const static char BITENGINE_SPECIAL_KEYWORD = '_';

static const NameSet BITMAP_EXPRESSION_AGGREGATE_FUNCTIONS{
    "bitmapcount",
    "bitmapmulticount",
    "bitmapextract",
    "bitmapmultiextract",
    "bitmapcountv2",
    "bitmapmulticountv2",
    "bitmapextractv2",
    "bitmapmultiextractv2"

    /// maybe support push down later
    // , "bitmapmulticountwithdate"
    // , "bitmapmultiextractwithdate"
    // , "bitmapmulticountwithdatev2"
    // , "bitmapmultiextractwithdatev2"
};

} // namespace DB
