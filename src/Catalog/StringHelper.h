#pragma once

#include <sstream>
#include <Core/Types.h>

namespace DB
{

namespace Catalog
{
/// Used to escape the origin string. Covert all '_' into '\_'
static String escapeString(const String & origin)
{
    std::stringstream escaped;
    for (std::string::const_iterator it = origin.begin(); it != origin.end(); it++)
    {
        std::string::value_type c = (*it);

        if (c == '\\' || c == '_')
            escaped << "\\" << c;
        else
            escaped << c;
    }

    return escaped.str();
}


}

}
