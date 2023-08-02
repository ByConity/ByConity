#include <brpc/server.h>
#include <common/ErrorHandlers.h>

#include "ILanguageServer.h"
#include "UDFImpl.h"
#include "UDFServer.h"
#include <unistd.h>
#include <chrono>
#include <thread>
namespace brpc {
    DECLARE_bool(usercode_in_pthread);
    DECLARE_int32(usercode_backup_threads);
}

namespace bthread {
    DECLARE_int32(bthread_concurrency);
}

int change_permissions(const char* filename) {
    // Get the original file permission
    struct stat file_stat;
    if (stat(filename, &file_stat) == -1) {
        // Handle error
        return -1;
    }
    mode_t original_mode = file_stat.st_mode;

    // Set group read and write permission
    
    mode_t new_mode = (original_mode | S_IRGRP | S_IWGRP) & ~S_IROTH;
    if (chmod(filename, new_mode) == -1) {
        std::cout << "Fail to change " << filename << " permission" << std::endl;
        // Handle error
        return -1;
    }

    std::cout << "Change " << filename << " permission successfully" << std::endl;
    return 0;

}

int UDFServer::main(const std::vector<std::string> &args)
{
    brpc::FLAGS_usercode_in_pthread = true;
    brpc::FLAGS_usercode_backup_threads = 1;
    bthread::FLAGS_bthread_concurrency = 1;

    brpc::Server server;

    if (args.size() < 1) {
        std::cout << "Usage Python_UDF_Server [UDS socket path]" << std::endl;
        exit(-1);
    } else {
        const char * uds = args[0].c_str();
        brpc::ServerOptions options;
        char rpc_path[PATH_MAX];

        rpc_uds_fmt(rpc_path, uds);
        if (remove(uds) == -1 && errno != ENOENT) {
            std::cerr << "Fail to remove socket file " << uds << std::endl;
            exit(-1);
        }
        auto uid = getuid();
        std::cout << "UDF server starts on " << uds << " " << std::to_string(uid) << std::endl;
        udf_service = std::make_unique<UDFImpl>(srv);
        if (server.AddService(udf_service.get(),
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            std::cerr << "Failed to add UDF service on " << uds << std::endl;
            exit(-1);
        }

        options.has_builtin_services = false;
        options.idle_timeout_sec = -1;
        options.max_concurrency = 1;
        options.num_threads = 1;

        if (server.Start(rpc_path, &options) != 0) {
            std::cerr << "Fail to start UDF server on " << args[0] << std::endl;
            exit(-1);
        }
        if (change_permissions(uds) < 0) {
            std::cerr << "Fail to modify permissions of sock at " << uds << std::endl;
            exit(-1);
        }
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    std::cout << "start pysrv!!!!!!!!" << std::endl;
    std::cout << "sta11rt pysrv!!!!!!!11!" << std::endl;
    server.RunUntilAskedToQuit();

    std::cout << "UDF server on " << args[0] << " stopped"  << std::endl;
    return 0;
}
