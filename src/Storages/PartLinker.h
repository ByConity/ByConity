#pragma once
#include <Core/Types.h>
#include <Core/Names.h>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class PartLinker
{
public:
    PartLinker(DiskPtr disk_,
               const String & new_part_path_,
               const String & source_part_path_,
               const NameSet & files_to_skip_,
               const NameToNameVector & files_to_rename_)
               : disk(disk_)
               , new_part_path(new_part_path_)
               , source_part_path(source_part_path_)
               , files_to_skip(files_to_skip_)
               , files_to_rename(files_to_rename_)
               {}

    void execute();

private:
    DiskPtr disk;
    String new_part_path;
    String source_part_path;
    NameSet files_to_skip;
    NameToNameVector files_to_rename;
};

}
