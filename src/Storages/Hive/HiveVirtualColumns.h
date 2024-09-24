#pragma once

#include "Core/Block.h"
#include "Core/NamesAndTypes.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"

namespace DB
{

NamesAndTypesList getHiveVirtuals();

void eraseHiveVirtuals(Block & block);

void addHiveVirtuals(Block & block, const HiveFilePtr & hive_file);

}
