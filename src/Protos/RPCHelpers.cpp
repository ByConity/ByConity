#include <Protos/RPCHelpers.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>

#include <brpc/controller.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOG_ERROR;
    extern const int BRPC_EXCEPTION;
}

namespace RPCHelpers
{
    constexpr auto unepxected_rare_exception = "rare_exception";

    void handleException(std::string * exception_str)
    {
        try
        {
            WriteBufferFromString out(*exception_str);
            writeException(*getSerializableException(), out, false);
        }
        catch (...)
        {
            exception_str->assign(unepxected_rare_exception);
        }
    }

    [[noreturn]] void checkException(const std::string & exception_str)
    {
        if (exception_str == unepxected_rare_exception)
            throw Exception("Service got a rare exception, but failed to send back", ErrorCodes::LOGICAL_ERROR);
        ReadBufferFromString in(exception_str);
        throw readException(in);
    }

    ContextMutablePtr createSessionContextForRPC(const ContextPtr & context, google::protobuf::RpcController & cntl_base)
    {
        auto & controller = static_cast<brpc::Controller &>(cntl_base);

        auto rpc_context = Context::createCopy(context);
        rpc_context->makeSessionContext();

        auto & client_info = rpc_context->getClientInfo();
        client_info.interface = ClientInfo::Interface::BRPC;
        client_info.current_address = Poco::Net::SocketAddress(butil::endpoint2str(controller.remote_side()).c_str());
        client_info.initial_address = client_info.current_address;

        return rpc_context;
    }
}
}
