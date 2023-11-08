#include <Common/ThreadStatus.h>

namespace DB
{

namespace DaemonManager
{

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
