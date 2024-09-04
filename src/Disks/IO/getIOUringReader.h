#pragma once

#include "common/types.h"
#include <Common/config.h>

#if USE_LIBURING

#include <Interpreters/Context_fwd.h>
#include <Disks/IO/IOUringReader.h>
#include <memory>

namespace DB
{

std::unique_ptr<IOUringReader> createIOUringReader(UInt32 sq_entries);

IOUringReader & getIOUringReaderOrThrow(ContextPtr);

IOUringReader & getIOUringReaderOrThrow();

}
#endif
