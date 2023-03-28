#include "CloudServices/ParallelReplicasReadingCoordinator.h"
#include "Common/ConcurrentBoundedQueue.h"
#include "CloudServices/ParallelReadRequestResponse.h"
#include "CloudServices/ParallelReadSplit.h"

namespace DB
{
class ParallelReplicasReadingCoordinator::ImplInterface
{
public:
    struct State
    {
        /// TODO: replace concurrent queue
        ConcurrentBoundedQueue<std::shared_ptr<IParallelReadSplit>> pending_splits;

        std::shared_ptr<IParallelReadSplit> getNext()
        {
            std::shared_ptr<IParallelReadSplit> n;
            if (pending_splits.pop(n))
                return n;
            else
                return {};
        }
    };

    std::vector<State> stats;

    ParallelReadResponse handleRequest(ParallelReadRequest request)
    {
        IParallelReadSplits splits;
        auto & stat = stats[request.replica_num];

        ParallelReadResponse response;
        while (splits.size() < request.min_number_of_slice)
        {
            auto s = stat.getNext();
            if (!s)
            {
                response.finish = true;
                break;
            }
            response.split.emplace_back(std::move(*s));
        }

        return response;
    }
};

ParallelReadResponse ParallelReplicasReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    if (!pimpl)
        initialize();

    return pimpl->handleRequest(std::move(request));
}

}
