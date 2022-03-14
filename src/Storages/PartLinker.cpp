#include <Storages/PartLinker.h>
#include <Disks/IDisk.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

void PartLinker::execute()
{
    /// Create hardlinks for unchanged files
    for (auto it = disk->iterateDirectory(source_part_path); it->isValid(); it->next())
    {
        if (files_to_skip.count(it->name()))
            continue;

        String destination = new_part_path;
        String file_name = it->name();
        auto rename_it = std::find_if(files_to_rename.begin(), files_to_rename.end(), 
                        [&file_name](const auto & rename_pair) { return rename_pair.first == file_name; });
        
        if (rename_it != files_to_rename.end())
        {
            if (rename_it->second.empty())
                continue;
            destination += rename_it->second;
        }
        else
        {
            destination += it->name();
        }

        if (!disk->isDirectory(it->path()))
            disk->createHardLink(it->path(), destination);
        else if (!startsWith("tmp_", it->name())) // ignore projection tmp merge dir
        {
            // it's a projection part directory
            disk->createDirectories(destination);
            for (auto p_it = disk->iterateDirectory(it->path()); p_it->isValid(); p_it->next())
            {
                String p_destination = destination + "/";
                String p_file_name = p_it->name();
                p_destination += p_it->name();
                disk->createHardLink(p_it->path(), p_destination);
            }
        }
    }
}

}
