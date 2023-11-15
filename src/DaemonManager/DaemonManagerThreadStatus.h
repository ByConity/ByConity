#include <Common/ThreadStatus.h>

namespace DB
{

namespace DaemonManager
{

/// This class can not be used inside brpc thread, because multiple brpc thread can shared a single normal thread, causing race condition
class DaemonManagerThreadStatus : public ThreadStatus
{
public:
    DaemonManagerThreadStatus()
        : ThreadStatus()
    {}

    void setQueryID(String query_id_)
    {
        query_id = std::move(query_id_);
    }

    ~DaemonManagerThreadStatus()
    {
        query_id.clear();
    }
};

}
}
