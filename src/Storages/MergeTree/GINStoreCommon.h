#pragma once

#include <stdint.h>
#include <Core/Types.h>

namespace DB
{

enum class GINStoreVersion: uint8_t
{
    v0 = 0,
    v1 = 1,
    v2 = 2
};

GINStoreVersion str2GINStoreVersion(const String& version_str_);

class IGinDataPartHelper;
using GinDataPartHelperPtr = std::unique_ptr<IGinDataPartHelper>;
using GinIndexPostingsList = roaring::Roaring;
using GinIndexPostingsListPtr = std::shared_ptr<GinIndexPostingsList>;

/// V0/V1/V2 Gin index segment descriptor, which contains:
struct GinIndexSegmentV0V1V2
{
    ///  Segment ID retrieved from next available ID from file .gin_sid
    UInt32 segment_id = 0;
    /// Start row ID for this segment
    UInt32 next_row_id = 0;
    /// .gin_post file offset of this segment's postings lists
    UInt64 postings_start_offset = 0;
    /// .gin_dict file offset of this segment's dictionaries
    UInt64 dict_start_offset = 0;
    /// total rows size in segment
    UInt64 total_row_size = 0;   
};

String v2StoreSegmentDictName(UInt32 segment_id_);

}
