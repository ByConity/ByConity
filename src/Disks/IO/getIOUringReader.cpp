#include <Disks/IO/getIOUringReader.h>

#if USE_LIBURING

#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

std::unique_ptr<IOUringReader> createIOUringReader(UInt32 sq_entries)
{
    return std::make_unique<IOUringReader>(sq_entries, false);
}

IOUringReader & getIOUringReaderOrThrow(ContextPtr context)
{
    auto & reader = context->getIOUringReader();
    if (!reader.isSupported())
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "io_uring is not supported by this system");
    }
    return reader;
}

IOUringReader & getIOUringReaderOrThrow()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");
    return getIOUringReaderOrThrow(context);
}

}
#endif
