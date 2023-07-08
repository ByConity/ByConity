#include <atomic>
#include <common/types.h>

namespace DB
{
template <typename ProfileElement>
class ProfileElementConsumer
{
public:
    explicit ProfileElementConsumer(std::string query_id_, UInt64 fetch_timeout_millseconds_ = 1)
        : is_running(true), query_id(query_id_), is_finish(false), fetch_timeout_millseconds(fetch_timeout_millseconds_)
    {
    }
    virtual ~ProfileElementConsumer() = default;

    virtual void consume(ProfileElement & element) = 0;
    std::string getQueryId() const {return query_id;}
    bool stillRunning() const {return is_running.load();}
    void stop() {is_running = false;}
    void finish() {is_finish = true;}
    bool isFinish() {return is_finish;}
    UInt64 getFetchTimeout() {return fetch_timeout_millseconds;}

private:
    std::atomic<bool> is_running;
    std::string query_id;
    bool is_finish;
    UInt64 fetch_timeout_millseconds;
};
}
