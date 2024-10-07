#include "IDisk.h"
#include "Disks/Executor.h"
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/Configurations.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_FILE_TYPE;
    extern const int READONLY;
}

std::atomic<UInt64> next_disk_id = 0;

bool IDisk::isDirectoryEmpty(const String & path)
{
    return !iterateDirectory(path)->isValid();
}

void copyFile(IDisk & from_disk, const String & from_path, IDisk & to_disk, const String & to_path)
{
    LOG_DEBUG(getLogger("IDisk"), "Copying from {} (path: {}) {} to {} (path: {}) {}.",
              from_disk.getName(), from_disk.getPath(), from_path, to_disk.getName(), to_disk.getPath(), to_path);

    auto in = from_disk.readFile(from_path);
    auto out = to_disk.writeFile(to_path);
    copyData(*in, *out);
    out->finalize();
}


using ResultsCollector = std::vector<std::future<void>>;

void asyncCopy(IDisk & from_disk, String from_path, IDisk & to_disk, String to_path, Executor & exec, ResultsCollector & results)
{
    if (from_disk.isFile(from_path))
    {
        auto result = exec.execute(
            [&from_disk, from_path, &to_disk, to_path]()
            {
                setThreadName("DiskCopier");
                DB::copyFile(from_disk, from_path, to_disk, fs::path(to_path) / fileName(from_path));
            });

        results.push_back(std::move(result));
    }
    else
    {
        fs::path dir_name = fs::path(from_path).parent_path().filename();
        fs::path dest(fs::path(to_path) / dir_name);
        to_disk.createDirectories(dest);

        for (auto it = from_disk.iterateDirectory(from_path); it->isValid(); it->next())
            asyncCopy(from_disk, it->path(), to_disk, dest, exec, results);
    }
}

void IDisk::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    auto & exec = to_disk->getExecutor();
    ResultsCollector results;

    asyncCopy(*this, from_path, *to_disk, to_path, exec, results);

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();
}

void IDisk::copyFiles(const std::vector<std::pair<String, String>> & files_to_copy, const std::shared_ptr<IDisk> & to_disk)
{
    auto & exec = to_disk->getExecutor();
    ResultsCollector results;

    for (const auto & [source_file, target_file]: files_to_copy)
    {
        if (!isFile(source_file))
            throw Exception(ErrorCodes::BAD_FILE_TYPE, "Can't copy {}, it's not a regular file", source_file);

        auto result = exec.execute(
            [this, source_file = source_file, &to_disk, target_file = target_file]()
            {
                setThreadName("DiskCopier");
                DB::copyFile(*this, source_file, *to_disk, target_file);
            });

        results.push_back(std::move(result));
    }

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();
}

void IDisk::truncateFile(const String &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate operation is not implemented for disk of type {}", getType());
}

void IDisk::assertNotReadonly(){
    if(unlikely(!is_disk_writable)){
        throw Exception("disk is not writable according to config 'enable_cnch_write_remote_disk' ", ErrorCodes::READONLY);
    }
}

void IDisk::setDiskWritable(){
    auto global_context = Context::getGlobalContextInstance();
    is_disk_writable = global_context->getRootConfig().enable_cnch_write_remote_disk;
}

SyncGuardPtr IDisk::getDirectorySyncGuard(const String & /* path */) const
{
    return nullptr;
}
}
