#pragma once

#include <Parsers/IParser.h>

namespace DB
{
class Field;

bool tryRewriteVwSettings(String &name, Field &value, IParser::Pos & pos);

}
