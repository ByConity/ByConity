#include <Interpreters/Context.h>
#include <Interpreters/SegmentScheduler.h>
#include <Protos/plan_segment_manager.pb.h>
#include <brpc/server.h>
#include <common/logger_useful.h>

namespace DB
{
class PlanSegmentManagerRpcService : public Protos::PlanSegmentManagerService
{
public:
    explicit PlanSegmentManagerRpcService(ContextPtr context_) : context(context_), log(&Poco::Logger::get("PlanSegmentManagerRpcService")) { }

    /// receive exception report send terminate query (coordinate host ---> segment executor host)
    void cancelQuery(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::CancelQueryRequest * request,
        ::DB::Protos::CancelQueryResponse * response,
        ::google::protobuf::Closure * done) override
    {
        brpc::ClosureGuard done_guard(done);
        auto mutable_context = Context::createCopy(context);
        auto cancel_code
            = mutable_context->getPlanSegmentProcessList().tryCancelPlanSegmentGroup(request->query_id(), request->coordinator_address());
        response->set_ret_code(std::to_string(static_cast<int>(cancel_code)));
    }

    /// fetch plan segment status (segment executor host --> coordinator host)
    void sendPlanSegmentStatus(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::SendPlanSegmentStatusRequest * request,
        ::DB::Protos::SendPlanSegmentStatusResponse * /*response*/,
        ::google::protobuf::Closure * done) override
    {
        brpc::ClosureGuard done_guard(done);
        RuntimeSegmentsStatus status(
            request->query_id(), request->segment_id(), request->is_succeed(), request->is_canceled(), request->message(), request->code());
        const SegmentSchedulerPtr & scheduler = context->getSegmentScheduler();
        scheduler->updateSegmentStatus(status);
        // this means exception happened during execution.
        if (!request->is_succeed() && !request->is_canceled())
        {
            try
            {
                scheduler->cancelPlanSegmentsFromCoordinator(request->query_id(), request->message(), context);
            }
            catch (...)
            {
                LOG_WARNING(log, "Call cancelPlanSegmentsFromCoordinator failed: " + getCurrentExceptionMessage(true));
            }
        }
        // todo  scheduler.cancelSchedule
    }

private:
    ContextPtr context;
    Poco::Logger * log;
};
}
