#include "IO/SeekableReadBuffer.h"

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
}

size_t copyFromIStreamWithProgressCallback(std::istream & istr, char * to, size_t n, const std::function<bool(size_t)> & progress_callback, bool * out_cancelled)
{
    const size_t chunk = DBMS_DEFAULT_BUFFER_SIZE;
    if (out_cancelled)
        *out_cancelled = false;

    size_t copied = 0;
    while (copied < n)
    {
        size_t to_copy = std::min(chunk, n - copied);
        istr.read(to + copied, to_copy);
        size_t gcount = istr.gcount();

        copied += gcount;

        bool cancelled = false;
        if (gcount && progress_callback)
            cancelled = progress_callback(copied);

        if (gcount != to_copy)
        {
            if (!istr.eof())
                throw Exception(
                    ErrorCodes::CANNOT_READ_FROM_ISTREAM,
                    "{} at offset {}",
                    istr.fail() ? "Cannot read from istream" : "Unexpected state of istream",
                    copied);

            break;
        }

        if (cancelled)
        {
            if (out_cancelled != nullptr)
                *out_cancelled = true;
            break;
        }
    }

    return copied;
}
}
