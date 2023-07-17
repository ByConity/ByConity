#pragma once

#include <memory>
#include <unordered_map>
#include "Common/ThreadPool.h"
#include "common/types.h"

#include "TCPQuery.h"

namespace DB
{

using TCPQueryHandleFunc = std::function<void(std::shared_ptr<TCPQuery>)>;

class AsyncQueryManager : public WithContext
{
public:
    explicit AsyncQueryManager(ContextWeakMutablePtr context_);

    // TODO(WangTao): use query id as id first, later we'll consider a better substitude.
    void insertAndRun(std::shared_ptr<TCPQuery> info, TCPQueryHandleFunc && func);
    void cancelQuery(String id);

private:
    std::unique_ptr<ThreadPool> pool;
};

} // namespace DB
