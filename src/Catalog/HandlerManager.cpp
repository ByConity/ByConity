#include <Catalog/HandlerManager.h>
#include <Catalog/StreamingHanlders.h>

namespace DB::Catalog
{

void HandlerManager::addHandler(const HandlerPtr & handler_ptr)
{
    std::lock_guard lock(mutex);
    auto it = handlers.insert(handlers.end(), handler_ptr);
    handler_ptr->handler_it = it;
}

void HandlerManager::removeHandler(const HandlerPtr & handler_ptr)
{
    std::lock_guard lock(mutex);
    handlers.erase(handler_ptr->handler_it);
}

}
