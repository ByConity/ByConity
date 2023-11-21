#pragma once

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Contexts.h>
#include "common/defines.h"

namespace DB
{
struct Handle
{
    std::shared_ptr<WaitContext> wait_context{nullptr};

    ALWAYS_INLINE explicit operator bool() const noexcept { return getInternal() != nullptr; }

    ALWAYS_INLINE std::shared_ptr<void> get() const noexcept { return getInternal(); }

protected:
    ALWAYS_INLINE std::shared_ptr<void> getInternal() const noexcept { return wait_context ? wait_context->get() : nullptr; }
};
}
