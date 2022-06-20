#pragma once

#include <Core/UUID.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsuggest-destructor-override"
#include <Protos/data_part.pb.h>       
#pragma GCC diagnostic pop

namespace DB
{

#define META_DATA_VERSION_PREFIX "VERSION"
#define META_DATA_STATUS_PREFIX "STATUS"
#define META_DATA_READY_FLAG "1"
#define META_DATA_PART_PREFIX "PT_"
#define META_DATA_PROJECTION_PREFIX "PRJ_"
#define META_DATA_WAL_PREFIX "WAL_"


using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

inline String getMetaStoreStatusKey(const UUID & uuid)
{
    return toString(uuid) + "_" + META_DATA_STATUS_PREFIX;
}

inline String getPartMetaPrefix(const UUID & uuid)
{
    return toString(uuid) + "_" + META_DATA_PART_PREFIX;
}

inline String getPartMetaKey(const UUID & uuid, const String & part_name)
{
    return getPartMetaPrefix(uuid) + part_name;
}

inline String getWALMetaPrefix(const UUID & uuid)
{
    return toString(uuid) + "_" + META_DATA_WAL_PREFIX;
}

inline String getWALMetaKey(const UUID & uuid, const String & wal_file)
{
    return getWALMetaPrefix(uuid) + wal_file;
}

inline String getProjectionPrefix(const UUID & uuid)
{
    return toString(uuid) + "_" + META_DATA_PROJECTION_PREFIX;
}

inline String getProjectionKey(const UUID & uuid, const String & parent_part, const String & prj_name)
{
    return getProjectionPrefix(uuid) + parent_part + "_" + prj_name;
}

String getSerializedPartMeta(const DataPartPtr & part);

MutableDataPartPtr buildPartFromMeta(const MergeTreeMetaBase & storage, const String & part_name, const Protos::DataPartModel & part_data);

MutableDataPartPtr buildProjectionFromMeta(const MergeTreeMetaBase & storage, const String & projection_name, const Protos::DataPartModel & part_data, const IMergeTreeDataPart * parent);

void deserializePartCommon(const Protos::DataPartModel & part_data, MutableDataPartPtr & part);

/** ----------------------- COMPATIBLE CODE BEGIN-------------------------- */
/*  compatible with old metastore. remove this later  */
String unescapeForDiskName(const String & s);
MutableDataPartPtr createPartFromRaw(const MergeTreeMetaBase & storage, const String & key, const String & meta);
/*  -----------------------  COMPATIBLE CODE END -------------------------- */

} // namespace name
