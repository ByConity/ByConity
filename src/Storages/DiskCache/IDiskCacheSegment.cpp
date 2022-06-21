#include "IDiskCacheSegment.h"

#include <Core/UUIDHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{
String IDiskCacheSegment::formatSegmentName(
    const String & uuid, const String & part_name, const String & column_name, UInt32 segment_number, const String & extension)
{
    WriteBufferFromOwnString wb;

    writeString(uuid, wb);
    writeChar('/', wb);
    writeString(part_name, wb);
    writeChar('/', wb);
    writeString(column_name, wb);
    writeChar('#', wb);
    writeIntText(segment_number, wb);
    writeString(extension, wb);

    return wb.str();
}
}
