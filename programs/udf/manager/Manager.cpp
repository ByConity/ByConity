#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <brpc/server.h>
#include <Poco/Util/ServerApplication.h>
#include <Common/Config/YAMLParser.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Protos/UDF.pb.h>
#include <Functions/UserDefined/Proto.h>
#include <Functions/UserDefined/FormatPath.h>
#include <Functions/UserDefined/SharedPath.h>
#include "Init.h"
#include "LoadBalancer.h"
#include "Logger.h"

namespace fs = std::filesystem;

using namespace Poco::XML;

class UDFManagerImpl : public DB::UDF
{
public:
    explicit UDFManagerImpl(std::unique_ptr<LoadBalancer> &&lb_) : lb(std::move(lb_)) {
        registerLoadBalancer(lb.get());
    }

    ~UDFManagerImpl() override {
        unregisterLoadBalancer();
        stopServers();
    }

private:
    template<typename T>
    void UDFCall(::google::protobuf::RpcController *rpc_ctl,
                 const T *req,
                 ::google::protobuf::Empty *,
                 ::google::protobuf::Closure* done)  {

        brpc::ClosureGuard done_guard(done);

        brpc::Controller* up = static_cast<brpc::Controller *>(rpc_ctl);
        bool feedback = true;
        brpc::Controller down;
        IClient *c;

        c = lb->SelectServer();

        if constexpr (std::is_same<T, DB::ScalarReq>::value)
            c->ScalarCall(&down, req);
        else if constexpr (std::is_same<T, DB::AggregateReq>::value)
            c->AggregateCall(&down, req);

        if (down.Failed()) {
            up->SetFailed(down.ErrorCode(), "%s", down.ErrorText().c_str());

            switch (down.ErrorCode()) {
                case brpc::Errno::ERPCTIMEDOUT:
                case brpc::Errno::EFAILEDSOCKET:
                case brpc::Errno::ELIMIT:
                    c->OnFatalError();
                    feedback = false;
            }
        }

        if (feedback)
            lb->Feedback(c);
    }

    void ScalarCall(::google::protobuf::RpcController *rpc_ctl,
                    const DB::ScalarReq *req,
                    ::google::protobuf::Empty *empty,
                    ::google::protobuf::Closure *done)  override {
        UDFCall(rpc_ctl, req, empty, done);
    }

    void AggregateCall(::google::protobuf::RpcController *rpc_ctl,
                       const DB::AggregateReq *req,
                       ::google::protobuf::Empty *empty,
                       ::google::protobuf::Closure *done) override {
        UDFCall(rpc_ctl, req, empty, done);
    }

    std::unique_ptr<LoadBalancer> lb;
};

class UDFManager: public Poco::Util::ServerApplication
{
public:
    void defineOptions(Poco::Util::OptionSet & _options) override {
        ServerApplication::defineOptions(_options);

        _options.addOption(
                           Poco::Util::Option("help", "h", "show help and exit")
                           .required(false)
                           .repeatable(false)
                           .binding("help"));

        _options.addOption(
                           Poco::Util::Option("path", "p", "path to UDF working directory")
                           .required(false)
                           .repeatable(false)
                           .argument("path", true)
                           .binding("path"));

        _options.addOption(
                           Poco::Util::Option("count", "n", "number of UDF servers to be started")
                           .required(false)
                           .repeatable(false)
                           .argument("count", true)
                           .binding("count"));

        _options.addOption(
                           Poco::Util::Option("config-file", "C", "set config file path")
                           .required(false)
                           .repeatable(false)
                           .argument("config-file", true)
                           .binding("config-file"));

    }

protected:
    int main(const std::vector<std::string> &) override;
    Poco::Util::XMLConfiguration *getConfig();
    Poco::Util::XMLConfiguration *processConfig();

private:
    std::unique_ptr<UDFManagerImpl> service;
};

Poco::Util::XMLConfiguration * UDFManager::processConfig()
{
    std::string path = config().getString("config-file", "config.xml");
    if (fs::exists(path))
    {
        fs::path p(path);
    
        std::string extension = p.extension();
        boost::algorithm::to_lower(extension);

        if (extension == ".yaml" || extension == ".yml")
        {
            return new Poco::Util::XMLConfiguration(DB::YAMLParser::parse(path));
        }
        else if (extension == ".xml" || extension == ".conf" || extension.empty())
        {
            return new Poco::Util::XMLConfiguration(path);
        }
        else
        {
            throw Poco::Exception("CANNOT_LOAD_CONFIG, Unknown format of '{}' config", path);
        }
    }
    
    throw Poco::Exception("FILE_NOT_EXIST, Unknown path of '{}' config", path);

}

Poco::Util::XMLConfiguration *UDFManager::getConfig()
{
    try {
        std::string path = config().getString("path");
        auto *c = new Poco::Util::XMLConfiguration;

        c->setString("udf_path", path);
        return c;
    } catch (...) {
        return processConfig();
    }
}

int UDFManager::main(const std::vector<std::string> &)
{
    brpc::Server server;

    {
        std::vector<std::unique_ptr<IClient>> clients;
        auto *cfg = getConfig();
        try {
            auto n = config().getUInt("count");
            cfg->setUInt("udf_processor.count", n);
        } catch (...) {
        }
        std::string udf_path = getUDFPath(cfg);
        std::string uds_path = getUDSPath(udf_path);
        brpc::ServerOptions options;
        Poco::Logger *log = &logger();
        char rpc_path[PATH_MAX];

        std::filesystem::create_directories(std::filesystem::path(udf_path));
        rpc_uds_fmt(rpc_path, uds_path.c_str());

        if (remove(uds_path.c_str()) == -1 && errno != ENOENT) {
            LOG_ERROR(log, std::string{"Fail to remove socket file "} + uds_path);
            return -1;
        }

        clients = startServers(cfg, log, udf_path.c_str());
        if (clients.empty()) {
            LOG_ERROR(log, "Fail to start UDF manager");
            return -1;
        }

        service = std::make_unique<UDFManagerImpl>(
            std::make_unique<LoadBalancer>(std::move(clients)));

        if (server.AddService(service.get(),
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG_ERROR(log, "Failed to add UDF service.");
            return -1;
        }

        options.has_builtin_services = false;
        options.idle_timeout_sec = -1;

        if (server.Start(rpc_path, &options) != 0) {
            LOG_ERROR(log, std::string{"Fail to start UDF manager server."} + rpc_path);
            return -1;
        }
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}

int main(int argc, char ** argv)
{
    // TODO: Add CAP_CHOWN and CAP_SETUID for udf-manager in clickhouse-install
    // Double check file permission such as shared memory file
    // and Python scripts should be isolated between UDF servers

    // For non-trusted CE collocated deployment, at least network namespace
    // should be isolated, in such no intranet should be accessed by UDF.
    // One of such solution is unshare command.
    UDFManager server;
    return server.run(argc, argv);
}
