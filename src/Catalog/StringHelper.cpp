#include <sstream>
#include <Catalog/StringHelper.h>
#include <Core/Types.h>
#include "common/types.h"
namespace DB
{

namespace Catalog
{
    /// Used to escape the origin string. Covert all '_' into '\_'
    String escapeString(const String & origin)
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

    // do the reverse of escapeString
    String unescapeString(const String & escaped)
    {
        std::ostringstream os;
        for (std::string::const_iterator it = escaped.begin(); it != escaped.end(); it++)
        {
            std::string::value_type c = (*it);

            if (c == '\\')
                it++;
            os << *it;
        }

        return os.str();
    }
}

}
