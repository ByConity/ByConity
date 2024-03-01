#include <Common/config.h>

#if USE_LASFS

#    include <map>
#    include <string>
#    include <IO/WriteBufferFromLasfs.h>
#    include <Lasfs/fs.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LASFS_OUTFILE_ERROR;
}

WriteBufferFromLasfs::WriteBufferFromLasfs(
    const std::map<std::string, std::string> settings_, const std::string & filepath_, const size_t buffer_size_, int flags_)
    : WriteBufferFromFileBase(buffer_size_, nullptr, 0)
    , settings(std::move(settings_))
    , filepath(filepath_)
    , flags(flags_)
    , buffer_size(buffer_size_)
{
    createAndSetLasfsBuilder();
    openConnectAndCreateFile();
}

void WriteBufferFromLasfs::nextImpl()
{
    if (!offset())
    {
        return;
    }

    size_t bytes_written = 0;
    int tmp = 0;

    while (bytes_written != offset())
    {
        tmp = las::lasfsWrite(fs, file, working_buffer.begin() + bytes_written, offset() - bytes_written);
        if (tmp == -1)
        {
            std::string err(las::lasfsGetLastError());
            throw Exception(err + " lasfsWrite() error", ErrorCodes::LASFS_OUTFILE_ERROR);
        }
        else
        {
            bytes_written += tmp;
        }
    }
}

std::string WriteBufferFromLasfs::getFileName() const
{
    return filepath;
}

void WriteBufferFromLasfs::createAndSetLasfsBuilder()
{
    builder = las::lasfsNewBuilder();
    for (const auto & setting : settings)
    {
        if (setting.first == "overwrite")
        {
            continue;
        }
        if (las::lasfsBuilderConfSetStr(builder, setting.first.c_str(), setting.second.c_str()))
        {
            std::string err(las::lasfsGetLastError());
            throw Exception(err + " lasfsBuilderConfSetStr() error", ErrorCodes::LASFS_OUTFILE_ERROR);
        }
    }
}

void WriteBufferFromLasfs::openConnectAndCreateFile()
{
    fs = las::lasfsBuilderConnect(nullptr, builder);
    if (fs == nullptr)
    {
        std::string err(las::lasfsGetLastError());
        throw Exception(err + " lasfsBuilderConnect() error", ErrorCodes::LASFS_OUTFILE_ERROR);
    }
    bool overwrite = false;
    if (settings["overwrite"] == "true")
    {
        overwrite = true;
    }
    file = las::lasfsOpenFile(fs, filepath.c_str(), flags, buffer_size, 0, 0, overwrite);
    if (file == nullptr)
    {
        std::string err(las::lasfsGetLastError());
        throw Exception(err + " lasfsOpenFile() error", ErrorCodes::LASFS_OUTFILE_ERROR);
    }
}

void WriteBufferFromLasfs::closeFileAndDisconnect()
{
    std::optional<String> close_file_err;
    std::optional<String> close_fs_err;
    if (file != nullptr && las::lasfsCloseFile(fs, file) == -1)
    {
        close_file_err = String(las::lasfsGetLastError());
    }
    file = nullptr;

    if (fs != nullptr && las::lasfsDisconnect(fs) == -1)
    {
        close_fs_err = String(las::lasfsGetLastError());
    }
    fs = nullptr;

    if (close_file_err.has_value() || close_fs_err.has_value())
    {
        throw Exception(
            fmt::format("Close file error {}, Close fs error {}", close_file_err.value_or("None"), close_fs_err.value_or("None")),
            ErrorCodes::LASFS_OUTFILE_ERROR);
    }
}

void WriteBufferFromLasfs::finalizeImpl()
{
    next();
    int ret = las::lasfsFlush(fs, file);
    if (ret == -1)
    {
        throw Exception(fmt::format("lasfsFlush error {}", String(las::lasfsGetLastError())), ErrorCodes::LASFS_OUTFILE_ERROR);
    }
    closeFileAndDisconnect();
}

WriteBufferFromLasfs::~WriteBufferFromLasfs()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}
}

#endif
