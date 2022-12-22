#pragma once
#include <brpc/stream.h>
#include <mutex>
#include <list>

namespace DB::Catalog
{

class StreamingHandlerBase;

class HandlerManager
{
public:
    using HandlerPtr = std::shared_ptr<StreamingHandlerBase>;
    using HandlerList = std::list<HandlerPtr>;

    void addHandler(const HandlerPtr & handler_ptr_);

    void removeHandler(const HandlerPtr & handler_ptr_);

private:
    HandlerList handlers;
    std::mutex mutex;
};

}
