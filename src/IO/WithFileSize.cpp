#include "WithFileSize.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDecorator.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FILE_SIZE;
}

template <typename T>
static size_t getFileSize(T & in)
{
    if (auto * with_file_size = dynamic_cast<WithFileSize *>(&in))
    {
        return with_file_size->getFileSize();
    }

    throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size");
}

size_t getFileSizeFromReadBuffer(ReadBuffer & in)
{
    if (auto * delegate = dynamic_cast<ReadBufferFromFileDecorator *>(&in))
    {
        return getFileSize(delegate->getWrappedReadBuffer());
    }

    return getFileSize(in);
}

bool isBufferWithFileSize(const ReadBuffer & in)
{
    if (const auto * delegate = dynamic_cast<const ReadBufferFromFileDecorator *>(&in))
    {
        return delegate->isWithFileSize();
    }

    return dynamic_cast<const WithFileSize *>(&in) != nullptr;
}

std::optional<size_t> tryGetFileSizeFromReadBuffer(ReadBuffer & in)
{
    try
    {
        return getFileSizeFromReadBuffer(in);
    }
    catch (...)
    {
        return std::nullopt;
    }
}

}
