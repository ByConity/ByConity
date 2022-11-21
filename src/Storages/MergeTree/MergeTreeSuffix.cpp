#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

bool isEngineReservedWord(const String & column)
{
    if (endsWith(column, COMPRESSION_COLUMN_EXTENSION))
        return true;
    return false;
}

} // namespace DB
