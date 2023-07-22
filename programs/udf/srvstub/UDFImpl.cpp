#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <google/protobuf/service.h>
#include <functional>
#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

// #include <common/Types.h>
// #include <Common/PODArray.h>
// #include <Common/UInt128.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Functions/UserDefined/Proto.h>
#include <Functions/UserDefined/VectorType.h>

#include "ILanguageServer.h"
#include "UDFImpl.h"
#include "UDFIColumn.h"
#include "UDFColumnArray.h"
#include "UDFColumnVector.h"
#include "UDFColumnString.h"
#include "UDFColumnFixedString.h"
#include "UDFColumnNullable.h"
#include "UDFColumnDecimal.h"


struct DesState {
    const struct DB::UDFMeta *metas;
    const char *extras;
    uint64_t prefix;
    uint16_t fd;
    bool isOut;
};

template<class T>
static T getNextExtra(struct DesState *st)
{
    const T *p = reinterpret_cast<const T *>(st->extras);

    st->extras += sizeof(T);
    return *p;
}

static UDFIColumn *DesColumn(struct DesState *st, uint64_t row_cnts)
{
    const struct DB::UDFMeta *meta = st->metas++;
    DB::TypeIndex type;

    type = static_cast<DB::TypeIndex>(meta->type);

    switch (type) {
#define V(IDX, TYPE) \
        case DB::TypeIndex::IDX: \
            return static_cast<UDFIColumn *>( \
                new UDFColumnVector<DB::TYPE>(type, st->prefix, st->fd++));
        APPLY_FOR_EACH_VECTOR_TYPE(V)
#undef V

        case DB::TypeIndex::String: {
            uint16_t offsetFd = st->fd++;
            uint16_t charFd = st->fd++;

            return static_cast<UDFIColumn *>(new UDFColumnString(
                type, st->prefix, offsetFd, charFd, row_cnts, st->isOut));
        }

        case DB::TypeIndex::Nullable: {
            uint16_t fd = st->fd++;
            UDFIColumn *nested;

            nested = DesColumn(st, row_cnts);
            if (!nested)
                throw std::runtime_error("nested type is null");

            return static_cast<UDFIColumn *>(
                new UDFColumnNullable(type, st->prefix, fd, nested));
        }

        case DB::TypeIndex::FixedString: {
            return static_cast<UDFIColumn *>(new UDFColumnFixedString(
                type, st->prefix, st->fd++, getNextExtra<uint64_t>(st)));
        }

        case DB::TypeIndex::Array: {
            uint16_t fd = st->fd++;
            UDFIColumn *nested;

            nested = DesColumn(st, row_cnts);
            if (!nested)
                throw std::runtime_error("nested type is null");

            return static_cast<UDFIColumn *>(
                new UDFColumnArray(type, st->prefix, fd, nested));
        }

        case DB::TypeIndex::Decimal32:
        case DB::TypeIndex::Decimal64:
        case DB::TypeIndex::Decimal128:
        case DB::TypeIndex::Tuple:
        case DB::TypeIndex::Set:
        case DB::TypeIndex::Interval:
        case DB::TypeIndex::Function:
        case DB::TypeIndex::AggregateFunction:
        case DB::TypeIndex::Map:
        case DB::TypeIndex::Enum8:
        case DB::TypeIndex::Enum16:
        case DB::TypeIndex::LowCardinality:
        default:
            throw std::runtime_error("unsupported type");
    }
}

void UDFImpl::ScalarCall(::google::protobuf::RpcController *rpc_ctl,
                         const DB::ScalarReq *req,
                         ::google::protobuf::Empty *rsp,
                         ::google::protobuf::Closure *done)
{   
    UDFCall<DB::ScalarReq>(rpc_ctl, req, rsp, done);
}

void UDFImpl::AggregateCall(::google::protobuf::RpcController *rpc_ctl,
                            const DB::AggregateReq *req,
                            ::google::protobuf::Empty *rsp,
                            ::google::protobuf::Closure *done)
{
    UDFCall<DB::AggregateReq>(rpc_ctl, req, rsp, done);
}

template<typename T>
void UDFImpl::UDFCall(::google::protobuf::RpcController *rpc_ctl,
                      const T *req,
                      ::google::protobuf::Empty *,
                      ::google::protobuf::Closure *done)
{   
    brpc::Controller* cntl = static_cast<brpc::Controller *>(rpc_ctl);
    brpc::ClosureGuard done_guard(done);

    const DB::ScalarReq * scalar_req;

    if constexpr (std::is_same<T, DB::AggregateReq>::value)
        scalar_req = &(req->scalar_params());
    else if constexpr (std::is_same<T, DB::ScalarReq>::value)
        scalar_req = req;

    struct DesState state = {
        .metas = reinterpret_cast<const struct DB::UDFMeta*>(scalar_req->metas().data()),
        .extras = scalar_req->extras().data(),
        .prefix = scalar_req->prefix(),
        .fd = 0,
        .isOut = true,
    };

    const struct DB::UDFMeta *end = state.metas + scalar_req->cnt();
    std::vector<std::unique_ptr<UDFIColumn>> inputs;
    std::unique_ptr<UDFIColumn> output;

    struct ILanguageServer::ScalarArgsCalc scalar_args = {
            .f_name = scalar_req->f_name(),
            .batch_f_id = DB::Method::SCALAR_CALL,
            .inputs = inputs,
            .output = nullptr,
            .row_counts = scalar_req->rows(),
            .setErr = [cntl](const std::string &msg){cntl->SetFailed(msg);},
            .version = scalar_req->version(),
    };

    try {
        if constexpr (std::is_same_v<T, DB::AggregateReq>) {
            // Deserialize output
            output.reset(DesColumn(&state, req->unique_states_count()));
            scalar_args.output = output.get();

            state.isOut = false;
            uint32_t batch_f_id = req->method();
            scalar_args.batch_f_id = batch_f_id;

            std::unique_ptr<UDFIColumn> state_map(nullptr);

            // Deserialize state_map for UDAF
            if (batch_f_id & (DB::ADD_BATCH | DB::MERGE_DATA_BATCH))
                state_map.reset(DesColumn(&state, scalar_req->rows()));

            // Deserialize input columns
            if (batch_f_id & DB::MERGE_DATA_BATCH) {
                inputs.push_back(std::unique_ptr<UDFIColumn>(DesColumn(&state, req->unique_states_count())));
                inputs.push_back(std::unique_ptr<UDFIColumn>(DesColumn(&state, scalar_req->rows())));
            } else {
                while (state.metas != end)
                    inputs.push_back(std::unique_ptr<UDFIColumn>(DesColumn(&state, scalar_req->rows())));
            }

            const struct ILanguageServer::AggregateArgsCalc aggregate_args = {
                .scalar_args = scalar_args,
                .state_map = state_map.get(),
                .state_counts = req->unique_states_count(),
            };

            srv->calc(aggregate_args);
        } else {
            // Deserialize output
            output.reset(DesColumn(&state, scalar_req->rows()));
            scalar_args.output = output.get();

            state.isOut = false;
            // Deserialize input columns
            while (state.metas != end)
                inputs.push_back(std::unique_ptr<UDFIColumn>(DesColumn(&state, scalar_req->rows())));

            srv->calc(scalar_args);
        }

    } catch (const std::runtime_error& error) {
        cntl->SetFailed(error.what());
    } catch (...) {
        cntl->SetFailed("Unknown exception");
    }
}

template void UDFImpl::UDFCall<DB::ScalarReq>(::google::protobuf::RpcController *rpc_ctl,
                                                   const DB::ScalarReq *req,
                                                   ::google::protobuf::Empty *,
                                                   ::google::protobuf::Closure *done);

template void UDFImpl::UDFCall<DB::AggregateReq>(::google::protobuf::RpcController *rpc_ctl,
                                                      const DB::AggregateReq *req,
                                                      ::google::protobuf::Empty *,
                                                      ::google::protobuf::Closure *done);
