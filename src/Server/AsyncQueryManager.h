#pragma once

#include <memory>
#include <unordered_map>
#include <Core/BackgroundSchedulePool.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ThreadPool.h>
#include <common/types.h>

namespace DB
{
using AsyncQueryHandlerFunc = std::function<void(String &, ASTPtr, ContextMutablePtr, ReadBuffer *)>;
using SendAsyncQueryIdCallback = std::function<void(const String &)>;

class AsyncQueryManager : public WithContext
{
public:
    explicit AsyncQueryManager(ContextWeakMutablePtr context_);
    ~AsyncQueryManager();

    void insertAndRun(
        String & query,
        ASTPtr ast,
        ContextMutablePtr context,
        ReadBuffer * istr,
        SendAsyncQueryIdCallback send_async_query_id,
        AsyncQueryHandlerFunc && async_query_handle_func);
    void cancelQuery(String id);

private:
    std::unique_ptr<ThreadPool> pool;
};

template <typename Signature>
struct make_copyable_function_helper;
template <typename R, typename... Args>
struct make_copyable_function_helper<R(Args...)>
{
    template <typename Input>
    std::function<R(Args...)> operator()(Input && i) const
    {
        auto ptr = std::make_shared<typename std::decay<Input>::type>(std::forward<Input>(i));
        return [ptr](Args... args) -> R { return (*ptr)(std::forward<Args>(args)...); };
    }
};

template <typename Signature, typename Input>
std::function<Signature> make_copyable_function(Input && i)
{
    return make_copyable_function_helper<Signature>()(std::forward<Input>(i));
}

} // namespace DB
