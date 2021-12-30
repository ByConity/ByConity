#include "MergeTreeDataPartVersions.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
void MergeTreeDataPartVersions::write(WriteBuffer & to)
{
    writeString("versions\n", to);
    writeString("enable_compact_map_data ", to);
    writeBoolText(enable_compact_map_data, to);
}

bool MergeTreeDataPartVersions::read(ReadBuffer & from, bool needCheckHeader)
{
    if (needCheckHeader)
    {
        assertString("versions\n", from);
    }
    assertString("enable_compact_map_data ", from);
    readBoolText(enable_compact_map_data, from);

    return true;
}

}
