#include <memory>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>

#include <IO/OpenedFile.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event FileOpen;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
}


void OpenedFile::open() const
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    file = File(file_name.c_str(), (flags == -1 ? 0 : flags) | O_RDONLY | O_CLOEXEC);
}

int OpenedFile::getFD() const
{
    std::lock_guard l(mutex);
    if (file.getFd() == -1)
        open();
    return file.getFd();
}

std::string OpenedFile::getFileName() const
{
    return file_name;
}


OpenedFile::OpenedFile(const std::string & file_name_, int flags_) : file_name(file_name_), flags(flags_)
{
}


OpenedFile::~OpenedFile()
{
    close(); /// Exceptions will lead to std::terminate and that's Ok.
}


void OpenedFile::close()
{
    std::lock_guard l(mutex);
    if (file.getFd() == -1)
        return;

    file.close();
    metric_increment.destroy();
}
}
