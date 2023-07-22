#include <array>
#include <cstdio>
#include <cstring>
#include <thread>

#include <fmt/core.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <Poco/Util/AbstractConfiguration.h>

#include "Init.h"
#include "LoadBalancer.h"
#pragma clang diagnostic ignored "-Walloca"
#include "PathEnv.h"
#include "populate.h"
#include "UDSClient.h"
#include "Logger.h"

struct clone_args {
    struct ProcessContext *ctx;
    int id;
};

struct ProcessContext {
    Poco::Logger *log;
    LoadBalancer *lb;
    std::unordered_map<pid_t, int> pids; /* PIDs of running UDF servers */
    std::string uds_base; /* unix domain socket base path */
    bool pendingSignal;   /* has pending signal in process */
    std::string bin;      /* UDF server binary */
};

static std::unique_ptr<struct ProcessContext> context;

void stopServers()
{
    for (auto & [pid, idx] : context->pids)
        kill(pid, SIGTERM);

    for (auto & [pid, idx] : context->pids) {
        pid_t p = pid;
        std::string s = fmt::format("Wait server {} with pid {} to be terminated...", idx, p);
        LOG_INFO(context->log, s);
        std::thread([p]() {
                    if(kill(p, 0) == 0)
                        kill(p, SIGKILL);
                    }).join();
    }
}

static void restartServer(IClient *c)
{
    const auto &pids = context->pids;
    int off = c->GetOffset();
    pid_t pid;

    const auto &it = find_if(pids.begin(), pids.end(),
                             [off](const auto &p) {
                                 return p.second == off;
                             });

    if (it == pids.end())
        return;

    pid = it->first;

    kill(pid, SIGTERM);

    /* force killing after 1 second */
    std::thread([pid]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));

                if(kill(pid, 0) == 0)
                    kill(pid, SIGKILL);
                }).detach();
}

static int startUDFSrv(void *arg)
{
    struct clone_args *args = static_cast<struct clone_args *>(arg);
    char *bin;
    char *uds;

    if (!args) {
        return -1;
    } else {
        char uds_path[PATH_MAX];

        UDSClient::udfsrv_uds_fmt(uds_path,
                                  args->ctx->uds_base.c_str(), args->id);
        uds = strdup(uds_path);
        bin = strdup(args->ctx->bin.c_str());
    }

    char *const execv_str[] = {bin, uds, nullptr};
    const char *is_ci_env = getenv("CI_PIPELINE_NAME");
    if (is_ci_env == nullptr) 
    {
        if (-1 == setuid(args->id + UID_BASE)) {
            std::string s = fmt::format("Failed to change UID for server {}: {}, system is not in secured mode.", args->id, strerror(errno));
            LOG_INFO(args->ctx->log, s);
        }
    }

    if (execvp(bin, execv_str) < 0) {
        std::string s = fmt::format("Failed to start server {}: {}", args->id, strerror(errno));
        LOG_INFO(args->ctx->log, s);
        return -1;
    }

    LOG_INFO(args->ctx->log, fmt::format("Started server {} at {}", args->id, uds));
    return 0;
}

static void prepare_env(Poco::Logger *log, const char *udf_path)
{
    const char *CIPATH = "/user/cnch_ci/";
    const char *hdfs_path = getenv("HDFS_PATH");
    char path[PATH_MAX];

    if (hdfs_path && !strncmp(hdfs_path, CIPATH, strlen(CIPATH))) {
        /* Limit OepnBLAS threads count to prevent running out of resource */
        LOG_INFO(log, "Running in CI env, limit # of openblas thread to 1");
        setenv("OPENBLAS_NUM_THREADS", "1", 0);
    }

    append_to_env("PATH", getcwd(path, PATH_MAX));
    LOG_INFO(log, std::string{"PATH: "} + getenv("PATH"));

    append_to_env("PYTHONPATH", udf_path);
    LOG_INFO(log, std::string{"PYTHONPATH: "} + getenv("PYTHONPATH"));
    strncpy(path, udf_path, PATH_MAX - 1);
    path[PATH_MAX - 1] = 0;
    const auto &ret = populate_binaries(path);
    if (ret)
        LOG_ERROR(log, *ret);
}

static int start_udf_servers(struct ProcessContext *ctx,
                             int offset, int server_number)
{
    #define STACK_SIZE (1024 * 1024)
    char *stackTop;
    char *stack;

    LOG_INFO(ctx->log, std::string{"Will start "} + std::to_string(server_number)
             + " UDF servers starts from " + std::to_string(offset));

    /* Allocate memory to be used for the stack of the child. */
    stack = static_cast<char *>(mmap(nullptr, STACK_SIZE, PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK, -1, 0));

    if (stack == MAP_FAILED) {
        LOG_ERROR(ctx->log, std::string{"Can not mmap "} + strerror(errno));
        return -1;
    }
    stackTop = stack + STACK_SIZE;  /* Assume stack grows downward */

    for (int id = offset; id < offset + server_number; id++) {
        struct clone_args args{ctx, id};

        pid_t pid = clone(startUDFSrv, stackTop,
                          CLONE_CHILD_CLEARTID | CLONE_CHILD_SETTID | SIGCHLD,
                          &args); 
        if (pid < 0) {
            LOG_ERROR(ctx->log, std::string{"Failed to created child: "} + strerror(errno));
            break;
        }

        /* add into PIDs mapping */
        ctx->pids[pid] = id;
    }

    munmap(stack, STACK_SIZE);

    return 0;
}

static std::string prepare_srv_bases(const Poco::Util::AbstractConfiguration *cfg)
{
    std::string srvs_base_path = cfg->getString("udf_processor.uds_path",
                                                "/tmp/udfsrvs");

    if (mkdir(srvs_base_path.c_str(), 0) == 0) {
        // 777 so that udf_processors can write the socket file when running with alternative uid.
        // :ToDo maybe setUid only after doing some initialization with root user in processor.
        (void)chmod(srvs_base_path.c_str(), 0777);
    }

    return srvs_base_path;
}

static void chld_handler(int)
{
    struct ProcessContext *ctx = context.get();

    if (!ctx)
        return;

    ctx->pendingSignal = true;

    while (true) {
        int status;

        pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid <= 0)
            break;

        /* been traced, do nothing */
        if (WIFSTOPPED(status) || WIFCONTINUED(status))
            continue;

        /* terminated */
        auto it = ctx->pids.find(pid);

        if (it == ctx->pids.end())
            continue;

        /* found pid */
        int offset = it->second;

        /* erase pid */
        ctx->pids.erase(it);

        /* restart the server at offset */
        start_udf_servers(ctx, offset, 1);

        /* make the server available */
        if (ctx->lb)
            ctx->lb->Feedback(offset);

        LOG_INFO(ctx->log, std::string{"Srv "} + std::to_string(offset) + " restarted");
    }

    ctx->pendingSignal = false;
}

void registerLoadBalancer(LoadBalancer *lb)
{
    context->lb = lb;
    /* monitor terminate signals for all UDF servers */
    signal(SIGCHLD, chld_handler);
}

void unregisterLoadBalancer()
{
    signal(SIGCHLD, SIG_DFL);

    while (context->pendingSignal)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    context->lb = nullptr;
}

std::vector<std::unique_ptr<IClient>>
startServers(const Poco::Util::AbstractConfiguration *cfg,
             Poco::Logger *log,
             const char *udf_path)
{
    context = std::make_unique<struct ProcessContext>();
    std::vector<std::unique_ptr<IClient>> clients;
    int server_number = 0;
    const char *base;

    server_number = cfg->getUInt("udf_processor.count",
                                 std::thread::hardware_concurrency());
    if (!server_number) {
        LOG_INFO(log, "UDF disabled");
        return clients;
    }

    context->log = log;
    context->bin = "pysrv";
    context->uds_base = prepare_srv_bases(cfg);
    prepare_env(log, udf_path);

    int ret = start_udf_servers(context.get(), 0, server_number);

    if (ret)
        return clients;

    /* create RPC clients */
    brpc::ChannelOptions options;
    options.timeout_ms = cfg->getUInt("udf_processor.timeout_ms", 10000);
    options.max_retry = cfg->getUInt("udf_processor.max_retry", 1);

    clients.reserve(server_number);
    base = context->uds_base.c_str();

    try {
        for (int i = 0; i < server_number; i++) {
            clients.emplace_back(std::make_unique<UDSClient>(i, base, &options,
                                 restartServer));
        }
    } catch (const std::runtime_error &err) {
        LOG_ERROR(log, err.what());
    } catch (...) {
        LOG_ERROR(log, "Failed to create UDF clients");
    }

    return clients;
}
