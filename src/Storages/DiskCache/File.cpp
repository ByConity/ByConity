#include <Storages/DiskCache/File.h>

#include <string>
#include <unistd.h>
#include <fmt/core.h>
#include <sys/file.h>
#include <Common/Exception.h>
#include <common/StringRef.h>
#include <common/defines.h>


namespace DB::ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
extern const int FILE_LOCK_ERROR;
}

namespace DB::HybridCache
{
File::File(int fd, bool owns_fd) noexcept : fd_(fd), owns_fd_(owns_fd)
{
    chassert(fd >= -1);
    chassert(fd != -1 || !owns_fd);
}

File::File(const char * name, int flags, mode_t mode) : fd_(::open(name, flags, mode)), owns_fd_(false)
{
    if (fd_ == -1)
        throwFromErrno(fmt::format("open {}, {} {} failed", name, flags, mode), ErrorCodes::CANNOT_OPEN_FILE);

    owns_fd_ = true;
}

File::File(const std::string & name, int flags, mode_t mode) : File(name.c_str(), flags, mode)
{
}

File::File(StringRef name, int flags, mode_t mode) : File(name.toString(), flags, mode)
{
}

File::File(File && other) noexcept : fd_(other.fd_), owns_fd_(other.owns_fd_)
{
    other.release();
}

File & File::operator=(File && other)
{
    closeNoThrow();
    swap(other);
    return *this;
}

File::~File()
{
    if (!closeNoThrow())
        chassert(errno != EBADF);
}

int File::release() noexcept
{
    int released = fd_;
    fd_ = -1;
    owns_fd_ = false;
    return released;
}

void File::swap(File & other) noexcept
{
    std::swap(fd_, other.fd_);
    std::swap(owns_fd_, other.owns_fd_);
}

void File::close()
{
    if (!closeNoThrow())
        throwFromErrno("close() failed", ErrorCodes::CANNOT_CLOSE_FILE);
}

bool File::closeNoThrow()
{
    int r = owns_fd_ ? ::close(fd_) : 0;
    release();
    return r == 0;
}

namespace
{
    int flockNoInt(int fd, int op)
    {
        ssize_t r;
        do
        {
            r = flock(fd, op);
        } while (r == -1 && errno == EINTR);
        return static_cast<int>(r);
    }
}

void File::lock()
{
    doLock(LOCK_EX);
}

bool File::tryLock()
{
    return doTryLock(LOCK_EX);
}

void File::lockShared()
{
    return doLock(LOCK_SH);
}

bool File::tryLockShared()
{
    return doTryLock(LOCK_SH);
}

void File::doLock(int op) const
{
    int r = flockNoInt(fd_, op);
    if (r == -1)
        throwFromErrno("flock() failed (lock)", ErrorCodes::FILE_LOCK_ERROR);
}

bool File::doTryLock(int op) const
{
    int r = flockNoInt(fd_, op | LOCK_NB);
    if (r == -1 && errno == EWOULDBLOCK)
        return false;
    if (r == -1)
        throwFromErrno("flock() failed (try_lock)", ErrorCodes::FILE_LOCK_ERROR);

    return true;
}

void File::unlock() const
{
    int r = flockNoInt(fd_, LOCK_UN);
    if (r == -1)
        throwFromErrno("flock() failed (unlock)", ErrorCodes::FILE_LOCK_ERROR);
}

void File::unlockShared() const
{
    unlock();
}

}
