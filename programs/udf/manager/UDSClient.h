#pragma once
#include <memory>
#include <optional>
#include <string>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <Functions/UserDefined/FormatPath.h>
#include <Functions/UserDefined/Proto.h>
#include <Protos/UDF.pb.h>

#include "IClient.h"
#include <unistd.h>


/* starting value of UID for UDF servers */
static const uint16_t UID_BASE = 58600;

namespace DB::ErrorCodes {
    extern const int BRPC_CANNOT_INIT_CHANNEL;
}

class UDSClient : public IClient
{
public:
    UDSClient(int offset_, const char *base, const brpc::ChannelOptions *options,
              std::function<void(IClient *)> cb) : fatal_cb(cb) {
        channel = std::make_unique<brpc::Channel>();
        char rpc_path[PATH_MAX];
        char uds[PATH_MAX - 8];

        udfsrv_uds_fmt(uds, base, offset_);
        rpc_uds_fmt(rpc_path, uds);

        if (0 != channel->Init(rpc_path, options))
            throw std::runtime_error(std::string("Failed to initialize UDS for socket file ") + uds);

        stub = std::make_unique<DB::UDF_Stub>(channel.get());
        offset = offset_;
        uid = UID_BASE + offset;
    }

    void updateFilePermission(const DB::ScalarReq *req, uid_t uuid, gid_t ggid) const {
        const auto *metas = reinterpret_cast<const struct DB::UDFMeta *>(req->metas().data());
        const char *shm_abspath = "/dev/shm/";
        uint16_t meta_size = req->cnt();
        char filename[PATH_MAX];
        uint16_t idx = 0;
        char *buf;
        strcpy(filename, shm_abspath);
        buf = filename + strlen(shm_abspath);

        for (uint16_t i = 0; i < meta_size; i++) {
            uint16_t fd_cnt = metas[i].fd_cnt;
            for (uint16_t j = 0; j < fd_cnt; j++) {
                shm_file_fmt(buf, req->prefix(), idx++);
                if (chown(filename,uuid, ggid) == -1) {
                    // :Todo Log the failure, and do nothing.
                    // Let it fail during execution.
                }
            }
        }

    }

    void ScalarCall(brpc::Controller *cntl, const DB::ScalarReq *req) override {
        google::protobuf::Empty rsp;

        if (uid) {
            updateFilePermission(req, uid, uid);
        }

        stub->ScalarCall(cntl, req, &rsp, nullptr);

        if (uid) {
            updateFilePermission(req, getuid(), getgid());
        }
    }

    void AggregateCall(brpc::Controller *cntl, const DB::AggregateReq *req) override {
        google::protobuf::Empty rsp;

        if (uid) {
            updateFilePermission(&req->scalar_params(), uid, uid);
        }

       
        stub->AggregateCall(cntl, req, &rsp, nullptr);

        if (uid) {
            updateFilePermission(&req->scalar_params(), getuid(), getgid());
        }
    }

    void OnFatalError() override {
        /* fatal error, need to restart the server */
        fatal_cb(this);
    }

    int GetOffset() const override { return offset; }

    static void udfsrv_uds_fmt(char *buf, const char *base, int offset)
    {
        sprintf(buf, "%s/%d.sock", base, offset);
    }

private:
    std::unique_ptr<brpc::Channel> channel;
    std::unique_ptr<DB::UDF_Stub> stub;
    int offset;
    uid_t uid;
    std::function<void(IClient *)> fatal_cb;
};
