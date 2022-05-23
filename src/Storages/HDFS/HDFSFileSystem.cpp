#include <algorithm>
#include <random>
#include <consul/bridge.h>
#include <Common/formatIPv6.h>
#include <Common/filesystemHelpers.h>
#include <Common/HostWithPorts.h>
#include <Poco/URI.h>
#include <Poco/Path.h>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <ServiceDiscovery/ServiceDiscoveryConsul.h>
#include <Storages/HDFS/HDFSFileSystem.h>
#include <hdfs/hdfs.h>
#include <common/logger_useful.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int HDFS_FILE_SYSTEM_UNREGISTER;
    extern const int HDFS_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace detail
{
    std::vector<std::string> SplitAndTrim(const std::string & ips)
    {
        std::vector<std::string> ret;
        boost::split(ret, ips, boost::is_any_of(","));
        for (size_t i = 0; i < ret.size(); i++)
        {
            boost::trim(ret[i]);
        }
        return ret;
    }
}

String addSchemeOnNeed(const String & ori, const String & prefix ) {
    if(ori.find("://") != std::string::npos){
        return ori;
    } else {
        return prefix + ori;
    }
}

HostWithPortsVec lookupNNProxy(const String & hdfs_service)
{
    HostWithPortsVec nnproxys;
    auto sd_consul = ServiceDiscoveryFactory::instance().tryGet(ServiceDiscoveryMode::CONSUL);
    if (sd_consul)
    {
        int retry = 0;
        do
        {
            if (retry++ > 2)
                throw Exception("No available nnproxy " + hdfs_service, ErrorCodes::NETWORK_ERROR);
            nnproxys = sd_consul->lookup(hdfs_service, ComponentType::NNPROXY);
        } while (nnproxys.size() == 0);
    }
    else
    {
        int retry = 0;
        do
        {
            if (retry++ > 2)
                throw Exception("No available nnproxy " + hdfs_service, ErrorCodes::NETWORK_ERROR);
            auto endpoints = cpputil::consul::lookup_name(hdfs_service);
            nnproxys = ServiceDiscoveryConsul::formatResult(endpoints, ComponentType::NNPROXY);
        } while (nnproxys.size() == 0);
    }
    return nnproxys;
}

void setHdfsHaConfig(
    HDFSBuilderPtr & builder,
    const String & service_name,
    const String & hdfs_user,
    const std::vector<std::pair<String, int>> & proxies_addr)
{
    hdfsBuilderSetUserName(builder.get(), hdfs_user.c_str());

    String service_uri = addSchemeOnNeed( service_name,"hdfs://");

    hdfsBuilderSetNameNode(builder.get(), service_uri.c_str());

    hdfsBuilderConfSetStr(builder.get(), "dfs.nameservices", service_name.c_str());

    // for ha case, the addrs of namenodes are configged in libhdfs3.xml.
    if (proxies_addr.empty())
        return;

    String nn_conf_prefix = "dfs.namenode.rpc-address." + service_name + ".";

    String service_conf_val;
    for (size_t i = 0; i < proxies_addr.size(); i++)
    {
        String nn_name = "nn" + std::to_string(i);

        service_conf_val += i == 0 ? nn_name : "," + nn_name;

        String nn_conf_key = nn_conf_prefix + nn_name;
        String nn_conf_val = std::get<0>(safeNormalizeHost( proxies_addr[i].first)) + ":" + std::to_string(proxies_addr[i].second);
        hdfsBuilderConfSetStr(builder.get(), nn_conf_key.c_str(), nn_conf_val.c_str());
    }
    String service_conf_key = "dfs.ha.namenodes." + service_name;
    hdfsBuilderConfSetStr(builder.get(), service_conf_key.c_str(), service_conf_val.c_str());
}


void setHdfsDirectConfig(HDFSBuilderPtr & builder, const String & hdfs_user, const String & host, int port)
{
    hdfsBuilderSetNameNode(builder.get(), host.c_str());
    hdfsBuilderSetNameNodePort(builder.get(), port);
    hdfsBuilderSetUserName(builder.get(), hdfs_user.c_str());
}


/*
 * nnproxy could be one of the following:
 * 1. nnproxy.service.lf, which is the psm of nnproxy. In this case, we shall use consul and fill in HA configs.
 * 2. cfs://a.b.c:65212, which is used by CFS proxy. In this case, we shall use defaultFs config.
 * 3. hdfs://a.b.c:652112, which is used to connect to hdfs namenode direclty. In this case ,we shall use defaultFs config.
 * 4. a1.b1.c1:65212,a2.b2.c2:65212, which is a serie of namenode ip separted by comma. It's used for HA connections.In this case, we shall fill in HA configs.
 */

HDFSBuilderPtr createHDFSBuilder(const Poco::URI & uri, const String & hdfs_user, const String & nnproxy)
{
    LOG_TRACE(&Poco::Logger::get(__func__), "params uri: {} hdfs_user: {} nnproxy: {} " , uri.toString(), hdfs_user, nnproxy);
    HDFSBuilderPtr builder(hdfsNewBuilder());
    if (builder == nullptr)
        throw Exception(
            "Unable to create builder to connect to HDFS: " + uri.toString() + " " + std::string(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    std::vector<std::pair<String, int>> proxies;
    String service_name;
    String host;
    unsigned short port = 65212;
    bool use_ha = true;

    if (uri.empty() || uri.getHost().empty() || uri.getPort() == 0)
    {
        if (nnproxy.find("://") != std::string::npos)
        {
            const Poco::URI proxy_uri(nnproxy);
            if (isHdfsOrCfsScheme(proxy_uri.getScheme()))
            {
                host = proxy_uri.getHost();
                port = proxy_uri.getPort();
                use_ha = false;
            }
        }

        if (use_ha && nnproxy.find(",") ==std::string::npos)
        {
            HostWithPortsVec nnproxys = lookupNNProxy(nnproxy);
            String proxies_str;
            for (const auto & host_with_port : nnproxys)
            {
                proxies_str += host_with_port.host + ":" + std::to_string(host_with_port.tcp_port) + ",";
                proxies.emplace_back(normalizeHost(host_with_port.host), host_with_port.tcp_port);
            }
            LOG_INFO(&Poco::Logger::get("HDFSFileSystem"), "Construct ha hdfs nn proxies {}", proxies_str);

            service_name = "nnproxy";
        } else if (use_ha) {
            // namenode addrs is directly passed in via "ip1,ip2,ip3"
            std::vector<std::string> addrs = detail::SplitAndTrim(nnproxy);
            for(auto & addr : addrs) {
                const Poco::URI proxy_uri(addr);
                proxies.emplace_back(normalizeHost(proxy_uri.getHost()),proxy_uri.getPort());
            }
            LOG_INFO(&Poco::Logger::get("HDFSFileSystem"), "Construct ha hdfs namenodes {}", nnproxy);
            service_name = "ha_namenodes";
        }
    }
    else
    {
        if (isHdfsOrCfsScheme(uri.getScheme())) {
            host = uri.getHost();
            port = uri.getPort();
            use_ha = false; // will build the connection using the defaultFs
        }
        else
        {
            proxies.push_back(std::pair<String, int>(std::get<0>(safeNormalizeHost(uri.getHost())), uri.getPort()));
            service_name = uri.getHost();
        }
    }



    hdfsBuilderSetUserName(builder.get(), hdfs_user.c_str());

    if (!use_ha)
    {
        hdfsBuilderSetNameNode(builder.get(), std::get<0>(safeNormalizeHost(host)).c_str());
        hdfsBuilderSetNameNodePort(builder.get(), port);
    }
    else
    {
        auto set_hdfs_ha_config
            = [](HDFSBuilderPtr & builder_, const String & service_name_, const std::vector<std::pair<String, int>> & proxies_addr) {
                  String service_uri = "hdfs://" + service_name_;
                  hdfsBuilderSetNameNode(builder_.get(), service_uri.c_str());

                  hdfsBuilderConfSetStr(builder_.get(), "dfs.nameservices", service_name_.c_str());

                  String nn_conf_prefix = "dfs.namenode.rpc-address." + service_name_ + ".";

                  String service_conf_val;
                  for (size_t i = 0; i < proxies_addr.size(); i++)
                  {
                      String nn_name = "nn" + std::to_string(i);

                      service_conf_val += i == 0 ? nn_name : "," + nn_name;

                      String nn_conf_key = nn_conf_prefix + nn_name;
                      String nn_conf_val = proxies_addr[i].first + ":" + std::to_string(proxies_addr[i].second);
                      hdfsBuilderConfSetStr(builder_.get(), nn_conf_key.c_str(), nn_conf_val.c_str());
                  }
                  String service_conf_key = "dfs.ha.namenodes." + service_name_;
                  hdfsBuilderConfSetStr(builder_.get(), service_conf_key.c_str(), service_conf_val.c_str());
              };

        set_hdfs_ha_config(builder, service_name, proxies);
    }
    return builder;
}

// HDFSFSPtr createHDFSFS(hdfsBuilder * builder)
// {
//     HDFSFSPtr fs(hdfsBuilderConnect(builder), detail::HDFSFsDeleter());
//     if (fs == nullptr)
//         throw Exception("Unable to connect to HDFS: " + std::string(hdfsGetLastError()),
//             ErrorCodes::NETWORK_ERROR);

//     return fs;
// }


HDFSFileSystem::HDFSFileSystem(
    const HDFSConnectionParams & hdfs_params_, int max_fd_num, int skip_fd_num, int io_error_num_to_reconnect)
    : fs(nullptr)
    , hdfs_params(hdfs_params_)
    , MAX_FD_NUM(max_fd_num)
    , SKIP_FD_NUM(skip_fd_num)
    , io_error_num(0)
    , IO_ERROR_NUM_TO_RECONNECT(io_error_num_to_reconnect)
    , fd_to_hdfs_file(MAX_FD_NUM + SKIP_FD_NUM, nullptr)
{
    //    HDFSBuilderPtr builder = createHDFSBuilder(Poco::URI(), hdfs_user, nnproxy);
    HDFSBuilderPtr builder = hdfs_params_.createBuilder(Poco::URI());
    fs = createHDFSFS(builder.get());
}

void HDFSFileSystem::Reconnect()
{
    //    HDFSBuilderPtr new_builder = createHDFSBuilder(Poco::URI(), hdfs_user, hdfs_nnproxy);
    HDFSBuilderPtr new_builder = hdfs_params.createBuilder(Poco::URI());
    HDFSFSPtr new_fs = createHDFSFS(new_builder.get());

    std::string host = hdfsBuilderGetNameNode(new_builder.get());

    {
        std::unique_lock<std::shared_mutex> lock(hdfs_mutex);
        fs = new_fs;
    }

    LOG_DEBUG(&Poco::Logger::get("HDFSFileSystem"), "Reconnect host: {}", host.data());
}

void HDFSFileSystem::ReconnectIfNecessary()
{
    if (IO_ERROR_NUM_TO_RECONNECT == 0 || (io_error_num != 0 && io_error_num % IO_ERROR_NUM_TO_RECONNECT == 0))
    {
        Reconnect();
        io_error_num = 0;
    }
}

int HDFSFileSystem::Open(const std::string& path, int flags, mode_t mode)
{
    (void)mode;

    HDFSFSPtr fs_copy = GetFS();
    hdfsFile fin = hdfsOpenFile(fs_copy.get(), path.c_str(), flags, 0, 0, 0);
    if (!fin) {
        HandleError(__PRETTY_FUNCTION__);
    }
    int fd = GetNextFd(path);
    fd_to_hdfs_file[fd] = fin;
    return fd;
}

// TODO: here we do not accquire the lock, is it OK?
// delay release owing to cpu cache flush
int HDFSFileSystem::Close(const int fd)
{
    if (fd < SKIP_FD_NUM || fd >= MAX_FD_NUM)
    {
        throw Exception("Illegal HDFS fd", ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }
    auto file = fd_to_hdfs_file[fd];

    HDFSFSPtr fs_copy = GetFS();
    int res = hdfsCloseFile(fs_copy.get(), file);
    if (res == -1) {
        HandleError(__PRETTY_FUNCTION__);
    }
    fd_to_hdfs_file[fd] = nullptr;
    return res;
}

int HDFSFileSystem::Flush(const int fd)
{
    HDFSFSPtr fs_copy = GetFS();
    auto file = GetHDFSFileByFd(fd);
    int ret = hdfsHFlush(fs_copy.get(), file);
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return ret;
}

ssize_t HDFSFileSystem::Read(const int fd, void *buf, size_t count)
{
    HDFSFSPtr fs_copy = GetFS();
    auto fin = GetHDFSFileByFd(fd);
    ssize_t ret = hdfsRead(fs_copy.get(), fin, buf, count);
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return ret;
}

ssize_t HDFSFileSystem::Write(const int fd, const void *buf, size_t count)
{
    HDFSFSPtr fs_copy = GetFS();
    auto file = GetHDFSFileByFd(fd);
    ssize_t ret = hdfsWrite(fs_copy.get(), file, buf, count);
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return ret;
}

bool HDFSFileSystem::Exists(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsExists(fs_copy.get(), path.c_str());
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return !ret;
}

bool HDFSFileSystem::Delete(const std::string& path, bool recursive)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsDelete(fs_copy.get(), path.c_str(), recursive);
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return !ret;
}

ssize_t HDFSFileSystem::GetSize(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs_copy.get(), path.c_str());
    if (!fileInfo) {
        HandleError(__PRETTY_FUNCTION__);
    }
    auto res = fileInfo->mSize;
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

ssize_t HDFSFileSystem::GetCapacity()
{
    HDFSFSPtr fs_copy = GetFS();
    ssize_t ret = static_cast<ssize_t>(hdfsGetCapacity(fs_copy.get()));
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return ret;
}

void HDFSFileSystem::List(const std::string& path,
    std::vector<std::string>& filenames)
{
    HDFSFSPtr fs_copy = GetFS();
    int num = 0;
    hdfsFileInfo* files = hdfsListDirectory(fs_copy.get(), path.c_str(), &num);
    if (!files)
    {
        HandleError(__PRETTY_FUNCTION__);
    }

    for (int i = 0; i < num; ++i)
    {
        Poco::Path filename(files[i].mName);
        filenames.push_back(filename.getFileName());
    }
    hdfsFreeFileInfo(files, num);
}

int64_t HDFSFileSystem::GetLastModifiedInSeconds(
    const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs_copy.get(), path.c_str());
    if (!fileInfo)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    auto res = fileInfo->mLastMod;
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::RenameTo(const std::string& path,
    const std::string& rpath)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsRename(fs_copy.get(), path.c_str(), rpath.c_str());
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return !ret;
}

bool HDFSFileSystem::CopyTo(const std::string& path,
    const std::string& rpath)
{
    HDFSFSPtr fs_copy = GetFS();
    // not supported, custom one
    {
        int ret = hdfsExists(fs_copy.get(), path.c_str());
        if (ret == -1)
        {
            HandleError(__PRETTY_FUNCTION__);
        }
        else if (ret == ENOENT)
        {
            throw Exception("Source file " + path + " not exist", ErrorCodes::HDFS_ERROR);
        }
    }
    {
        int ret = hdfsExists(fs_copy.get(), rpath.c_str());
        if (ret == -1)
        {
            HandleError(__PRETTY_FUNCTION__);
        }
        else if (ret == 0)
        {
            throw Exception("Target file " + rpath + " already exist", ErrorCodes::HDFS_ERROR);
        }
    }
    auto fin = hdfsOpenFile(fs_copy.get(), path.c_str(), O_RDONLY, 0, 0, 0);
    if (!fin) {
        HandleError(__PRETTY_FUNCTION__);
    }
    SCOPE_EXIT({
        hdfsCloseFile(fs_copy.get(), fin);
    });
    auto fout = hdfsOpenFile(fs_copy.get(), rpath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!fout)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    SCOPE_EXIT({
        hdfsCloseFile(fs_copy.get(), fout);
    });

    char buf[10240];
    while (true)
    {
        int ret = hdfsRead(fs_copy.get(), fin, buf, 10240);
        if (ret > 0)
        {
            int write_ret = hdfsWrite(fs_copy.get(), fout, buf, ret);
            if (write_ret == -1)
            {
                HandleError(__PRETTY_FUNCTION__);
            }
        }
        else if (ret == -1)
        {
            HandleError(__PRETTY_FUNCTION__);
        }
        else if (ret == 0)
        {
            break;
        }
    }

    return true;
}

bool HDFSFileSystem::MoveTo(const std::string& path,
    const std::string& rpath)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsRename(fs_copy.get(), path.c_str(), rpath.c_str());
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return !ret;
}

bool HDFSFileSystem::CreateFile(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFile file = hdfsOpenFile(fs_copy.get(), path.c_str(), O_WRONLY, 0, 0, 0);
    if (!file)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    if (hdfsCloseFile(fs_copy.get(), file))
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return true;
}

bool HDFSFileSystem::CreateDirectory(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsCreateDirectory(fs_copy.get(), path.c_str());
    if (ret == -1)
        HandleError(__PRETTY_FUNCTION__);
    return !ret;
}

/// Creates a directory(and all parent directories if necessary).
bool HDFSFileSystem::CreateDirectories(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsCreateDirectoryEx(fs_copy.get(), path.c_str(), 493, 1);
    if (ret == -1)
        HandleError(__PRETTY_FUNCTION__);
    return !ret;
}

int HDFSFileSystem::Seek(const int fd, off_t offset, int whence)
{
    if (whence != SEEK_SET)
    {
        throw Exception("Illegal HDFS fd", ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }

    HDFSFSPtr fs_copy = GetFS();
    auto file = GetHDFSFileByFd(fd);
    if (hdfsSeek(fs_copy.get(), file, offset))
    {
        HandleError(__PRETTY_FUNCTION__);
    }

    return offset;
}

int HDFSFileSystem::GetNextFd(const std::string& path) {
    // SKIP_FD_NUM to (SKIP_FD_NUM + MAX_FD_NUM)
    int index = std::hash<std::string>{}(path) % MAX_FD_NUM;
    auto dumpy = 0;
    auto i = 0;
    // if flag_ has been set by others, we wait for next cycle
    while (flag_.test_and_set(std::memory_order_acquire))
    {
        i++;
        if (i < 32)
        {
            continue;
        }
        else if (i < 100)
        {
            dumpy = i;
            dumpy++;
        }
        else
        {
            // wait so long
            std::this_thread::yield();
        }
    }

    // acquire the flag_ lock
    while (fd_to_hdfs_file[index] != nullptr) {
        index = (index + 1) % MAX_FD_NUM;
    }
    // unlock the flag_
    flag_.clear(std::memory_order_release);

    return index + SKIP_FD_NUM;
}

HDFSFSPtr HDFSFileSystem::GetFS()
{
    ReconnectIfNecessary();

    HDFSFSPtr fs_copy = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
        fs_copy = fs;
    }
    return fs_copy;
}

void HDFSFileSystem::HandleError(const String & func)
{
    if (errno == EIO)
    {
        ++io_error_num;
    }

    const char* underlying_err_msg = hdfsGetLastError();
    std::string underlying_err_str = underlying_err_msg ? std::string(underlying_err_msg) : "";

    switch (errno)
	{
       case EIO:
           throw Poco::IOException(underlying_err_str, errno);
       case EPERM:
           throw Poco::FileAccessDeniedException(underlying_err_str, errno);
       case EACCES:
           throw Poco::FileAccessDeniedException(underlying_err_str, errno);
       case ENOENT:
           throw Poco::FileNotFoundException(underlying_err_str, errno);
       case ENOTDIR:
           throw Poco::OpenFileException(underlying_err_str, errno);
       case EISDIR:
           throw Poco::OpenFileException(underlying_err_str, errno);
       case EROFS:
           throw Poco::FileReadOnlyException(underlying_err_str, errno);
       case EEXIST:
           throw Poco::FileExistsException(underlying_err_str, errno);
       case ENOSPC:
           throw Poco::FileException(underlying_err_str, errno);
       case EDQUOT:
           throw Poco::FileException(underlying_err_str, errno);
#if !defined(_AIX)
        case ENOTEMPTY:
            throw Poco::DirectoryNotEmptyException(underlying_err_str, errno);
#endif
        case ENAMETOOLONG:
            throw Poco::PathSyntaxException(underlying_err_str, errno);
        case ENFILE:
        case EMFILE:
            throw Poco::FileException(underlying_err_str, errno);
        case ENOTSUP:
            throw Poco::FileException(underlying_err_str, errno);
        case HDFS_EFAILED:
            throw Poco::FileException(func + " failed", errno);
        default:
            throw Poco::FileException(underlying_err_str, errno);
	}
}

bool HDFSFileSystem::IsFile(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs_copy.get(), path.c_str());
    if (!fileInfo) {
        HandleError(__PRETTY_FUNCTION__);
    }
    auto res = (fileInfo->mKind == kObjectKindFile);
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::IsDirectory(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs_copy.get(), path.c_str());
    if (!fileInfo) {
        HandleError(__PRETTY_FUNCTION__);
    }
    auto res = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::SetWriteable(const std::string& path, bool flag)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs_copy.get(), path.c_str());
    if (!fileInfo) {
        HandleError(__PRETTY_FUNCTION__);
    }
    auto mode = (fileInfo->mPermissions);
    hdfsFreeFileInfo(fileInfo, 1);

    if (flag)
    {
        mode = mode | S_IWUSR;
    }
    else
    {
        mode_t wmask = S_IWUSR | S_IWGRP | S_IWOTH;
		mode = mode & ~wmask;
    }

    if (hdfsChmod(fs_copy.get(), path.c_str(), mode))
    {
        HandleError(__PRETTY_FUNCTION__);
    }

    return true;
}

bool HDFSFileSystem::CanExecute(const std::string& path)
{
    HDFSFSPtr fs_copy = GetFS();
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs_copy.get(), path.c_str());
    if (!fileInfo) {
        HandleError(__PRETTY_FUNCTION__);
    }
    auto res = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::SetSize(const std::string& path, uint64_t size)
{
    HDFSFSPtr fs_copy = GetFS();
    int wait;
    int ret = hdfsTruncate(fs_copy.get(), path.c_str(), size, &wait);
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return !ret;
}

bool HDFSFileSystem::SetLastModifiedInSeconds(const std::string& path, uint64_t ts)
{
    HDFSFSPtr fs_copy = GetFS();
    int ret = hdfsUtime(fs_copy.get(), path.c_str(), ts, -1);
    if (ret == -1)
    {
        HandleError(__PRETTY_FUNCTION__);
    }
    return !ret;
}

static fs_ptr<DB::HDFSFileSystem> defaultHdfsFileSystem = nullptr;
//static std::unique_ptr<DB::HDFSFileSystem> nullHdfsFileSystem = nullptr;
void registerDefaultHdfsFileSystem(
    const HDFSConnectionParams & hdfs_params, const int max_fd_num, const int skip_fd_num, const int io_error_num_to_reconnect)
{
    // force ha mode.
    HDFSConnectionParams ha_params = hdfs_params;
    ha_params.setNNProxyHa(true);
    defaultHdfsFileSystem = std::make_shared<DB::HDFSFileSystem>(ha_params, max_fd_num, skip_fd_num, io_error_num_to_reconnect);
}

fs_ptr<DB::HDFSFileSystem> & getDefaultHdfsFileSystem()
{
    if (defaultHdfsFileSystem == nullptr)
        throw Exception("Not register HDFSFileSystem", ErrorCodes::HDFS_FILE_SYSTEM_UNREGISTER);
    return defaultHdfsFileSystem;
}

namespace HDFSCommon
{

// here is the function used by ClickHouse
int open(const char *pathname, int flags, mode_t mode)
{
    return getDefaultHdfsFileSystem()->Open(pathname, flags, mode);
}

int close(int fd)
{
    return getDefaultHdfsFileSystem()->Close(fd);
}

off_t lseek(int fd, off_t offset, int whence)
{
    return getDefaultHdfsFileSystem()->Seek(fd, offset, whence);
}

int fsync(int fd)
{
    return getDefaultHdfsFileSystem()->Flush(fd);
}

ssize_t read(const int fd, void *buf, size_t count)
{
    return getDefaultHdfsFileSystem()->Read(fd, buf, count);
}

int write(int fd, const void* buf, size_t count)
{
    return getDefaultHdfsFileSystem()->Write(fd, buf, count);
}

bool exists(const std::string& path)
{
    return getDefaultHdfsFileSystem()->Exists(path);
}

bool remove(const std::string& path, bool recursive)
{
    return getDefaultHdfsFileSystem()->Delete(path, recursive);
}

int fcntl(int fd, int cmd, ... /* arg */ )
{
    (void)fd;
    (void)cmd;
    return -1;
}

ssize_t getCapacity() {
    return getDefaultHdfsFileSystem()->GetCapacity();
}

ssize_t getSize(const std::string& path)
{
    return getDefaultHdfsFileSystem()->GetSize(path);
}

bool renameTo(const std::string& path, const std::string& rpath)
{
    return getDefaultHdfsFileSystem()->RenameTo(path, rpath);
}

bool copyTo(const std::string& path, const std::string& rpath)
{
    return getDefaultHdfsFileSystem()->CopyTo(path, rpath);
}

bool moveTo(const std::string& path, const std::string& rpath)
{
    return getDefaultHdfsFileSystem()->MoveTo(path, rpath);
}

bool createFile(const std::string& path)
{
    return getDefaultHdfsFileSystem()->CreateFile(path);
}

bool createDirectory(const std::string& path)
{
    return getDefaultHdfsFileSystem()->CreateDirectory(path);
}

bool createDirectories(const std::string& path)
{
    return getDefaultHdfsFileSystem()->CreateDirectories(path);
}

Poco::Timestamp getLastModified(const std::string& path)
{
    int64_t seconds = getDefaultHdfsFileSystem()->GetLastModifiedInSeconds(
        path);
    // timestamp in microseconds
    return Poco::Timestamp(seconds * 1000 * 1000);
}

void setLastModified(const std::string& path, const Poco::Timestamp& ts)
{
    uint64_t seconds = ts.epochTime();
    getDefaultHdfsFileSystem()->SetLastModifiedInSeconds(path, seconds);
}

void list(const std::string& path, std::vector<std::string>& files)
{
    getDefaultHdfsFileSystem()->List(path, files);
}

bool isFile(const std::string& path)
{
    return getDefaultHdfsFileSystem()->IsFile(path);
}

bool isDirectory(const std::string& path)
{
    return getDefaultHdfsFileSystem()->IsDirectory(path);
}

bool canExecute(const std::string& path)
{
    return getDefaultHdfsFileSystem()->CanExecute(path);
}

void setReadOnly(const std::string& path)
{
    getDefaultHdfsFileSystem()->SetWriteable(path, false);
}

void setWritable(const std::string& path)
{
    getDefaultHdfsFileSystem()->SetWriteable(path, true);
}

void setSize(const std::string& path, uint64_t size)
{
    getDefaultHdfsFileSystem()->SetSize(path, size);
}

DirectoryIterator::DirectoryIterator()
{
}

DirectoryIterator::DirectoryIterator(const std::string& dir_path_): dir_path(joinPaths({dir_path_}, true))
{
    OpenDir();
    dir_path.setFileName(GetCurrent());
    file = dir_path;
}

DirectoryIterator::DirectoryIterator(const HDFSCommon::File& file_): dir_path(joinPaths({file_.path()}, true))
{
    OpenDir();
    dir_path.setFileName(GetCurrent());
    file = dir_path;
}

DirectoryIterator::DirectoryIterator(const Poco::Path& other_path): dir_path(joinPaths({other_path.toString()}, true))
{
    OpenDir();
    dir_path.setFileName(GetCurrent());
    file = dir_path;
}

DirectoryIterator::~DirectoryIterator()
{
    CloseDir();
}

void DirectoryIterator::OpenDir()
{
    file_names.clear();
    HDFSCommon::list(dir_path.toString(), file_names);
    current_idx = -1;
    Next();
}

void DirectoryIterator::CloseDir()
{
    file_names.clear();
}

bool DirectoryIterator::hasNext() const
{
    return current.size() != 0;
}

const std::string& DirectoryIterator::Next()
{
    ++current_idx;
    if (current_idx >= file_names.size())
    {
        current.clear();
    }
    else
    {
        current = file_names[current_idx];
    }

    dir_path.setFileName(current);
    file = dir_path;

    return GetCurrent();
}

DirectoryIterator & DirectoryIterator::operator++()
{
    Next();
    return *this;
}

const std::string & DirectoryIterator::GetCurrent() const { return current; }


}

HDFSConnectionParams::HDFSConnectionParams()
{
    conn_type = CONN_DUMMY;
}

HDFSConnectionParams::HDFSConnectionParams(HDFSConnectionType t, const String & hdfs_user_, const String & hdfs_service_)
    : conn_type(t), hdfs_user(hdfs_user_), hdfs_service(hdfs_service_), addrs()
{
    lookupOnNeed();
}

HDFSConnectionParams::HDFSConnectionParams(HDFSConnectionType t, const String & hdfs_user_, const std::vector<IpWithPort>& addrs_ )
    : conn_type(t), hdfs_user(hdfs_user_), hdfs_service(""), addrs(addrs_)
{
    lookupOnNeed();
}

HDFSConnectionParams HDFSConnectionParams::parseHdfsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    static String HDFS_USER = "hdfs_user";
    static String CFS_ADDR = "cfs_addr";
    static String HDFS_ADDR = "hdfs_addr";
    static String HDFS_HA = "hdfs_ha_nameservice";
    static String HDFS_NNPROXY = "hdfs_nnproxy";

    String hdfs_user = config.getString(HDFS_USER, "clickhouse");

    if (config.has(CFS_ADDR))
    {
        // for ip:port, poco cannot parse it correctly. the ip will be parsed as scheme.
        Poco::URI cfs_uri(addSchemeOnNeed( config.getString(CFS_ADDR), "cfs://"));
        String host = cfs_uri.getHost();
        int port = cfs_uri.getPort() == 0 ? 65212 : cfs_uri.getPort();
        return HDFSConnectionParams(CONN_CFS, hdfs_user, {{host, port}});
    }
    else if (config.has(HDFS_ADDR))
    {
        Poco::URI hdfs_uri(addSchemeOnNeed(config.getString(HDFS_ADDR), "hdfs://"));
        String host = hdfs_uri.getHost();
        int port = hdfs_uri.getPort() == 0 ? 65212 : hdfs_uri.getPort();
        return HDFSConnectionParams(CONN_HDFS, hdfs_user, {{host, port}});
    }
    else if (config.has(HDFS_HA))
    {
        return HDFSConnectionParams(CONN_HA, hdfs_user, config.getString(HDFS_HA));
    }

    // hdfs_nnproxy could refer to both cfs and nnproxy.
    String hdfs_nnproxy = config.getString("hdfs_nnproxy", "nnproxy");
    if (hdfs_nnproxy.find("://") != String::npos)
    {
        // this could be a cfs or hdfs uri like cfs://preonline.com:65212/
        const Poco::URI proxy_uri(addSchemeOnNeed(hdfs_nnproxy, "hdfs://"));
        HDFSConnectionType conn_type = isCfsScheme(proxy_uri.getScheme()) ? CONN_CFS : CONN_HDFS;
        String host = proxy_uri.getHost();
        int port = proxy_uri.getPort() == 0 ? 65212 : proxy_uri.getPort();
        return HDFSConnectionParams(conn_type, hdfs_user, {{host, port}});
    }
    else
    {
        // this is a nnproxy.
        return HDFSConnectionParams(CONN_NNPROXY, hdfs_user, hdfs_nnproxy);
    }
    // return defaultNNProxy();
}

void HDFSConnectionParams::lookupOnNeed()
{
    thread_local static std::random_device rd;
    thread_local static std::mt19937 g(rd());
    if (conn_type != CONN_NNPROXY)
        return;

    HostWithPortsVec nnproxys = lookupNNProxy(hdfs_service);
    assert(nnproxys.size() > 0);
    for (auto it : nnproxys)
    {
        addrs.emplace_back( it.host, it.tcp_port);
    }
    std::uniform_int_distribution<size_t> dist(0, addrs.size()-1);
    // ensure the nnproxy picked is not the same as the broken one. 
    int retry = 3;
    do {
        size_t new_index = dist(g); 
        nnproxy_index = (new_index == nnproxy_index) ? ((nnproxy_index + 1)% addrs.size()): new_index;
        if(!brokenNNs.isBrokenNN(addrs[nnproxy_index].first)) {
            break;
        }
    } while(retry-- > 0);
    inited = true; 
}

void HDFSConnectionParams::setNNProxyBroken()
{
    if( conn_type != CONN_NNPROXY || !inited) {
        return;
    }
    brokenNNs.insert(addrs[nnproxy_index].first);
}

Poco::URI HDFSConnectionParams::formatPath(const String & path) const
{
    Poco::URI uri(path);
    switch (conn_type)
    {
        case CONN_NNPROXY: {
            uri.setScheme("hdfs");
            if (use_nnproxy_ha)
            {
                uri.setHost(hdfs_service);
            }
            else
            {
                uri.setHost(addrs[nnproxy_index].first);
                uri.setPort(addrs[nnproxy_index].second);
            }
            return uri;
        }
        case CONN_CFS: {
            uri.setScheme("cfs");
            uri.setHost(addrs[0].first);
            uri.setPort(addrs[0].second);
            return uri;
        }
        case CONN_HA: {
            uri.setScheme("hdfs");
            uri.setHost(hdfs_service);
            uri.setPort(0);
            return uri;
        }
        case CONN_HDFS: {
            uri.setScheme("hdfs");
            uri.setHost(addrs[0].first);
            uri.setPort(addrs[0].second);
            return uri;
        }
        case CONN_DUMMY: {
            return uri;
        }
    }

    return uri;
}

HDFSBuilderPtr HDFSConnectionParams::createBuilder(const Poco::URI & uri) const
{
    // construct from uri.
    // uri is hdfs://host:ip/a/b or hdfs://my-hadoop/a/b

    auto raw_builder = hdfsNewBuilder();
    if (raw_builder == nullptr)
        throw Exception("Unable to create HDFS builder, maybe hdfs3.xml missing" , ErrorCodes::BAD_ARGUMENTS);
    // set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large

    HDFSBuilderPtr builder(raw_builder);
    if (!uri.getHost().empty())
    {
        auto normalizedHost =  std::get<0>(safeNormalizeHost(uri.getHost()));
        if (uri.getScheme() == "cfs")
        {
            setHdfsDirectConfig(builder, hdfs_user, "cfs://" + normalizedHost, uri.getPort());
            return builder;
        }
        else if (uri.getScheme() == "hdfs" && uri.getPort() != 0)
        {
            setHdfsDirectConfig(builder, hdfs_user, "hdfs://" + normalizedHost, uri.getPort());
            return builder;
        }
        else if (uri.getScheme() == "hdfs")
        {
            setHdfsHaConfig(builder, normalizedHost, hdfs_user, std::vector<std::pair<String, int>>());
            return builder;
        }
    }

    // construt from other configs.
    switch (conn_type)
    {
        case CONN_NNPROXY: {
            if (use_nnproxy_ha)
            {
                setHdfsHaConfig(builder, hdfs_service, hdfs_user, addrs);
                return builder;
            }
            else
            {
                IpWithPort targetNode = addrs[nnproxy_index];
                setHdfsDirectConfig(builder, hdfs_user, "hdfs://" + std::get<0>(safeNormalizeHost(targetNode.first)), targetNode.second);
                return builder;
            }
        }
        case CONN_HDFS: {
            String host = std::get<0>(safeNormalizeHost( addrs[0].first));
            int port = addrs[0].second;
            if (!isHdfsScheme(addrs[0].first.substr(0, 4)))
            {
                host = "hdfs://" + host;
            }
            setHdfsDirectConfig(builder, hdfs_user, host, port);
            return builder;
        }
        case CONN_CFS: {
            String host =std::get<0>(safeNormalizeHost( addrs[0].first));
            int port = addrs[0].second;
            if (!isCfsScheme(addrs[0].first.substr(0, 3)))
            {
                host = "cfs://" + host;
            }
            setHdfsDirectConfig(builder, hdfs_user, host, port);
            return builder;
        }
        case CONN_HA: {
            setHdfsHaConfig(builder, hdfs_service, hdfs_user, addrs);
            return builder;
        }
        case CONN_DUMMY: {
            throw Exception("create builder on uninitialized HDFSConecitonParams", ErrorCodes::LOGICAL_ERROR);
        }
    }
    return builder;
}

String HDFSConnectionParams::toString() const
{
    std::stringstream ss;
    switch (conn_type)
    {
        case CONN_DUMMY:
            poco_assert_msg(false, "uninitialized hdfs connection params");
            break;
        case CONN_HDFS:
            ss << "[type = conn_hdfs,"
               << " user = " << hdfs_user << ", addr = " << addrs[0].first << ":" << addrs[0].second << "]";
            break;
        case CONN_CFS:
            ss << "[type = conn_cfs,"
               << " user = " << hdfs_user << ", addr = " << addrs[0].first << ":" << addrs[0].second << "]";
            break;
        case CONN_HA:
            ss << "[type = ha, addrs = ";
            for (size_t i = 0; i < addrs.size(); ++i)
            {
                ss << addrs[i].first << ":" << addrs[i].second;
                if (i != addrs.size())
                {
                    ss << ",";
                }
            }
            ss << "]";
            break;
        case CONN_NNPROXY:
            ss << "[type = conn_nnproxy, "
               << "user = " << hdfs_user << ", nnproxy = " << hdfs_service << "]";
            break;
        // default:
        //     ss << "unknown connection type";
    }
    return ss.str();
}


}
