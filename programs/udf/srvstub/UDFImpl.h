#pragma once
#include <Protos/UDF.pb.h>
#include <cstdint>
#include <common/logger_useful.h>

class ILanguageServer;
class UDFIColumn;

class UDFImpl : public DB::UDF
{
public:
    explicit UDFImpl(ILanguageServer *lgs): srv(lgs), log{&Poco::Logger::get("UDF_Python_Server")} {}

private:
    void ScalarCall(::google::protobuf::RpcController *cntl,
                    const DB::ScalarReq *req,
                    ::google::protobuf::Empty *rsp,
                    ::google::protobuf::Closure *done) override;

    void AggregateCall(::google::protobuf::RpcController *cntl,
                    const DB::AggregateReq *req,
                    ::google::protobuf::Empty *rsp,
                    ::google::protobuf::Closure *done) override;

    template<typename T>
    void UDFCall(::google::protobuf::RpcController *cntl,
                    const T *req,
                    ::google::protobuf::Empty *rsp,
                    ::google::protobuf::Closure *done);

    ILanguageServer *srv;
    Poco::Logger * log;
};
