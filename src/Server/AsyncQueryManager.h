#pragma once

#include <memory>
#include <unordered_map>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include "Common/ThreadPool.h"
#include "common/types.h"

#include "DataStreams/IBlockStream_fwd.h"
#include "TCPQuery.h"

namespace DB
{
using TCPQueryHandleFunc = std::function<void(std::shared_ptr<TCPQuery>)>;
using HttpQueryHandlerFunc = std::function<void(BlockIO, ASTPtr, ContextMutablePtr, const std::optional<FormatSettings> &)>;

class AsyncQueryManager : public WithContext
{
public:
    explicit AsyncQueryManager(ContextWeakMutablePtr context_);

    void insertAndRun(std::shared_ptr<TCPQuery> info, TCPQueryHandleFunc && func);
    void insertAndRun(
        BlockIO streams,
        ASTPtr ast,
        ContextMutablePtr context,
        WriteBuffer & ostr,
        const std::optional<FormatSettings> & output_format_settings,
        HttpQueryHandlerFunc && func);
    static void sendAsyncQueryId(
        const String & id, ContextMutablePtr context, WriteBuffer & ostr, const std::optional<FormatSettings> & output_format_settings);
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
