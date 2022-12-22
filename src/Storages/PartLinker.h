#pragma once
#include <Core/Types.h>
#include <Core/Names.h>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class Block;
class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
struct IMergeTreeIndex;
using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;
struct IMergeTreeProjection;
using MergeTreeProjectionPtr = std::shared_ptr<const IMergeTreeProjection>;

/***
 * link files from source_part_path to new_part_path,
 * need a files_to_skip / files_to_rename to indicate which files should not
 * be linked and which files should be renamed after link.
 */
class PartLinker
{
public:
    PartLinker(
        DiskPtr disk_,
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

    static NameSet collectFilesToSkip(
        const MergeTreeDataPartPtr & source_part,
        const Block & updated_header,
        const std::set<MergeTreeIndexPtr> & indices_to_recalc,
        const String & mrk_extension,
        const std::set<MergeTreeProjectionPtr> & projections_to_recalc,
        bool update_delete_bitmap);

private:
    DiskPtr disk;
    String new_part_path;
    String source_part_path;
    NameSet files_to_skip;
    NameToNameVector files_to_rename;
};

}
