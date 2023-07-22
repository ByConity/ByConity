#pragma once

namespace brpc {
    class Controller;
}

namespace DB {
    class ScalarReq;
    class AggregateReq;
}

class IClient {
public:
    virtual int GetOffset() const = 0;

    virtual void ScalarCall(brpc::Controller *cntl, const DB::ScalarReq *req) = 0;

    virtual void AggregateCall(brpc::Controller *cntl, const DB::AggregateReq *req) = 0;

    virtual void OnFatalError() = 0;

    virtual ~IClient() = default;
};
