#include <Storages/NexusFS/NexusFSInodeManager.h>

#include <atomic>
#include <cstddef>

#include <google/protobuf/util/delimited_message_util.h>
#include <Poco/String.h>

#include <Protos/disk_cache.pb.h>
#include "common/defines.h"
#include "common/logger_useful.h"
#include "common/types.h"
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include "IO/WriteHelpers.h"

namespace ProfileEvents
{
extern const Event NexusFSInodeManagerLookupMicroseconds;
extern const Event NexusFSInodeManagerInsertMicroseconds;
}

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int CANNOT_OPEN_FILE;
}

namespace DB::NexusFSComponents
{

std::shared_ptr<BlockHandle> FileMeta::getHandle(UInt64 segment_id)
{
    std::lock_guard l(mutex);
    if (segment_id >= segments.size())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "FileMeta::getHandle for segment_id {} out of bound", segment_id);
    if (segments[segment_id] && !segments[segment_id]->isRelAddressValid())
        segments[segment_id].reset();
    return segments[segment_id];
}

void FileMeta::setHandle(UInt64 segment_id, std::shared_ptr<BlockHandle> & handle)
{
    std::lock_guard l(mutex);
    if (segment_id >= segments.size())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "FileMeta::setHandle for segment_id {} out of bound", segment_id);
    segments[segment_id] = handle;
}

void FileMeta::toProto(Protos::NexusFSFileMeta * proto)
{
    std::lock_guard l(mutex);
    proto->set_file_size(file_size);
    for (size_t i = 0; i < segments.size(); i++)
    {
        const auto & handle = segments[i];
        if (handle)
        {
            Protos::NexusFSFileSegment * proto_handle = proto->add_segments();
            proto_handle->set_segment_id(i);
            proto_handle->set_address_rid(handle->getRelAddress().rid().index());
            proto_handle->set_address_offset(handle->getRelAddress().offset());
            proto_handle->set_size(handle->getSize());
        }
    }
}

bool FileMeta::canBeRemoved()
{
    bool has_valid_handle = false;
    std::lock_guard l(mutex);
    for (auto & segment : segments)
    {
        if (segment)
        {
            if (segment->isRelAddressValid())
                has_valid_handle = true;
            else
                segment.reset();
        }
    }
    return has_valid_handle;
}

std::pair<UInt64, UInt64> FileMeta::getCachedSizeAndSegments()
{
    std::lock_guard l(mutex);
    UInt64 cached_segments = 0;
    UInt64 cached_size = 0;
    for (auto & segment : segments)
    {
        if (segment)
        {
            if (segment->isRelAddressValid())
            {
                cached_segments++;
                cached_size += segment->getSize();
            }
            else
                segment.reset();
        }
    }
    return {cached_size, cached_segments};
}


std::shared_ptr<BlockHandle> Inode::getHandle(String & file, UInt64 segment_id)
{
    auto it = files.find(file);
    if (it == files.end())
        return nullptr;

    const auto & meta = it->second;
    if (!meta)
        return nullptr;

    return meta->getHandle(segment_id);
}

void Inode::setHandle(
    const String & file,
    UInt64 segment_id,
    std::shared_ptr<BlockHandle> & handle,
    const std::function<std::pair<size_t, UInt32>()> & get_file_and_segment_size,
    std::atomic<UInt64> & num_file_metas)
{
    auto it = files.find(file);
    if (it == files.end())
    {
        auto [file_size, segment_size] = get_file_and_segment_size();
        auto meta = std::make_shared<FileMeta>(file_size, segment_size);
        it = files.try_emplace(file, meta).first;
        num_file_metas++;
    }

    const auto & meta = it->second;
    if (!meta)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "FileMeta for file {} not found", file);

    meta->setHandle(segment_id, handle);
}

void Inode::setHandle(const String & file, std::shared_ptr<FileMeta> & file_meta)
{
    if (!files.try_emplace(file, file_meta).second)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "FileMeta for file {} already exists", file);
}

void Inode::cleanInvalidFiles(std::atomic<UInt64> & num_file_metas)
{
    for (auto [file_name, file_meta] : files)
    {
        if (file_meta->canBeRemoved())
        {
            files.erase(file_name);
            num_file_metas--;
        }
    }
}

void Inode::toProto(Protos::NexusFSInode * node)
{
    node->set_node_id(id);
    for (const auto & [key, meta] : files)
    {
        Protos::NexusFSFileMeta * file = node->add_files();
        file->set_file_name(key);
        meta->toProto(file);
    }
}

void Inode::getFileCachedStates(std::vector<FileCachedState> & result)
{
    for (const auto & [file_name, file_meta] : files)
    {
        FileCachedState state;
        auto [total_size, total_segment] = file_meta->getTotalSizeAndSegments();
        auto [cached_size, cached_segment] = file_meta->getTotalSizeAndSegments();
        result.emplace_back(FileCachedState{
            .file_path = file_name,
            .total_segments = total_segment,
            .cached_segments = cached_segment,
            .total_size = total_size,
            .cached_size = cached_size});
    }
}


std::shared_ptr<BlockHandle> InodeManager::lookup(const String & path, UInt64 segment_id) const
{
    // TODO: increase hits
    return peek(path, segment_id);
}

std::shared_ptr<BlockHandle> InodeManager::peek(const String & path, UInt64 segment_id) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::NexusFSInodeManagerLookupMicroseconds);

    std::vector<String> dirs;
    resolvePath(path, dirs);
    chassert(!dirs.empty());

    String file = dirs.back();
    dirs.pop_back();

    UInt64 pid = 0;
    auto it = inodes.end();
    for (auto & dir : dirs)
    {
        String pid_dir = toString(pid) + "/" + dir;
        it = inodes.find(pid_dir);
        if (it == inodes.end())
        {
            return nullptr;
        }
        else
        {
            pid = it->second->getId();
        }
    }

    const auto & inode = dirs.empty() ? root_inode : it->second;
    return inode->getHandle(file, segment_id);
}

void InodeManager::insert(
    const String & path,
    UInt64 segment_id,
    std::shared_ptr<BlockHandle> & handle,
    const std::function<std::pair<size_t, UInt32>()> & get_file_and_segment_size)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::NexusFSInodeManagerInsertMicroseconds);

    std::vector<String> dirs;
    resolvePath(path, dirs);

    String file = dirs.back();
    dirs.pop_back();

    UInt64 pid = 0;
    auto it = inodes.end();
    for (const auto & dir : dirs)
    {
        String pid_dir = toString(pid) + "/" + dir;
        it = inodes.find(pid_dir);
        if (it == inodes.end())
        {
            it = inodes.try_emplace(pid_dir, std::make_shared<Inode>(inode_id.fetch_add(1))).first;
            num_inodes++;
        }
        pid = it->second->getId();
    }

    const auto & inode = dirs.empty() ? root_inode : it->second;
    inode->setHandle(file, segment_id, handle, get_file_and_segment_size, num_file_metas);
}

void InodeManager::reset()
{
    inodes.clear();
    num_inodes = 1;
}

void InodeManager::persist(google::protobuf::io::CodedOutputStream * stream) const
{
    Protos::NexusFSInodeManager manager;
    manager.set_prefix(prefix);
    auto * root_inode_proto = manager.mutable_root_inode();
    root_inode_proto->set_node_key("");
    root_inode->toProto(root_inode_proto);
    for (const auto & [key, val] : inodes)
    {
        auto * node = manager.add_inodes();
        node->set_node_key(key);
        val->toProto(node);
    }
    google::protobuf::util::SerializeDelimitedToCodedStream(manager, stream);
}

void InodeManager::recover(
    google::protobuf::io::CodedInputStream * stream, HybridCache::RegionManager & region_manager, std::atomic<UInt64> & num_segments)
{
    Protos::NexusFSInodeManager manager;
    google::protobuf::util::ParseDelimitedFromCodedStream(&manager, stream, nullptr);

    if (manager.prefix() != prefix)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Invalid prefix . Expected prefix: {}, actual prefix: {}",
            prefix,
            manager.prefix());

    auto recover_files_in_inode = [&](std::shared_ptr<Inode> & node, const Protos::NexusFSInode & proto_node)
    {
        for (const auto & proto_file : proto_node.files())
            {
                auto file = std::make_shared<FileMeta>(proto_file.file_size(), segment_size);
                num_file_metas++;
                for (const auto & proto_seg : proto_file.segments())
                {
                    auto rid = HybridCache::RegionId(proto_seg.address_rid());
                    auto addr = RelAddress(rid, proto_seg.address_offset());
                    auto handle = std::make_shared<BlockHandle>(addr, proto_seg.size());
                    file->setHandle(proto_seg.segment_id(), handle);
                    region_manager.getRegion(rid).addHandle(handle);
                    num_segments++;
                }
                node->setHandle(proto_file.file_name(), file);
            }
    };

    recover_files_in_inode(root_inode, manager.root_inode());
    for (const auto & proto_node : manager.inodes())
    {
        auto inode = std::make_shared<Inode>(proto_node.node_id());
        num_inodes++;
        recover_files_in_inode(inode, proto_node);
        inodes.emplace(proto_node.node_key(), inode);
    }
}


String InodeManager::extractValidPath(const String & path) const
{
    if (path.size() <= prefix.size())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "path {} invalid, its length is smaller than prefix", path);
    if (!prefix.empty() && prefix != path.substr(0, prefix.size()))
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "path {} has invalid prefix, required prefix should be {}", path, prefix);

    String valid_path = path.substr(prefix.size());
    if (valid_path.empty())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "path {} is invalid, it consists of only prefix. ", path);

    return valid_path;
}


void InodeManager::resolvePath(const String & path, std::vector<String> & resolved_dirs) const
{
    String valid_path = extractValidPath(path);

    String dir;
    for (auto ch : valid_path)
    {
        if (ch == '/')
        {
            if (!dir.empty())
            {
                resolved_dirs.push_back(dir);
                dir.clear();
            }
        }
        else
        {
            dir.push_back(ch);
        }
    }
    if (!dir.empty())
        resolved_dirs.push_back(dir);

    if (resolved_dirs.empty())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "path {} is invalid, it consists of only prefix. ", path);

    // Since all of the part files are named as "data", we concat the file name and its directory name as "xxx?data",
    // in order to avoid one hashmap lookup.
    // We use '?' because it is not a valid character in file name.  
    if (Poco::toLower(resolved_dirs.back()) == "data" && resolved_dirs.size() > 1)
    {
        resolved_dirs.pop_back();
        resolved_dirs.back() += "?data";
    }

    // {
    //     String str = resolved_dirs[0];
    //     for (size_t i = 1; i < resolved_dirs.size(); i++)
    //     {
    //         str += "," + resolved_dirs[i];
    //     }
    //     LOG_TRACE(log, "resolvePath get: {}", str);
    // }
}

void InodeManager::cleanInvalidFiles()
{
    for (auto [_, node] : inodes)
        node->cleanInvalidFiles(num_file_metas);
}

std::vector<FileCachedState> InodeManager::getFileCachedStates()
{
    std::vector<FileCachedState> ret;
    root_inode->getFileCachedStates(ret);
    for (auto [dir_name, node] : inodes)
        node->getFileCachedStates(ret);
    return ret;
}
}
