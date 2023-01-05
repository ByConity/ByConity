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
    extern const int BRPC_TIMEOUT;
    extern const int BRPC_EXCEPTION;
    extern const int BRPC_HOST_DOWN;
    extern const int BRPC_CONNECT_ERROR;
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
        rpc_context->makeQueryContext();

        auto & client_info = rpc_context->getClientInfo();
        client_info.interface = ClientInfo::Interface::BRPC;
        client_info.current_address = Poco::Net::SocketAddress(butil::endpoint2str(controller.remote_side()).c_str());
        client_info.initial_address = client_info.current_address;

        return rpc_context;
    }

    void assertController(const brpc::Controller & cntl)
    {
        if (!cntl.Failed())
            return;

        auto err = cntl.ErrorCode();

        if (err == ECONNREFUSED || err == ECONNRESET || err == ENETUNREACH)
        {
            throw Exception(std::to_string(err) + ":" + cntl.ErrorText(), ErrorCodes::BRPC_CONNECT_ERROR);
        }
        else if (err == EHOSTDOWN)
        {
            /// TODO: handle more error codes, temporarily remove EHOSTDOWN error https://github.com/apache/incubator-brpc/issues/936
            throw Exception(std::to_string(err) + ":" + cntl.ErrorText(), ErrorCodes::BRPC_HOST_DOWN);
        }
        else if (err == brpc::Errno::ERPCTIMEDOUT)
        {
            throw Exception(std::to_string(err) + ":" + cntl.ErrorText(), ErrorCodes::BRPC_TIMEOUT);
        }
        else /// Should we throw exception here to cover all other errors?
            throw Exception(std::to_string(err) + ":" + cntl.ErrorText(), ErrorCodes::BRPC_EXCEPTION);
    }
}

}
