#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>

namespace DB
{

NamesAndTypesList getHiveVirtuals();

void eraseHiveVirtuals(Block & block);

void addHiveVirtuals(Block & block, const LakeScanInfoPtr & lake_scan_info);

}
