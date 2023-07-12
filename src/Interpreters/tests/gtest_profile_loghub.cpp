#include <memory>
#include <vector>
#include <Interpreters/profile/ProfileLogHub.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <gtest/gtest.h>
#include "common/types.h"
#include "Common/ThreadPool.h"

namespace DB{

class MockConsumer : public ProfileElementConsumer<ProcessorProfileLogElement>
{
public:
    explicit MockConsumer(std::string query_id):ProfileElementConsumer(query_id) {}
    ~MockConsumer() override;
    void consume(ProcessorProfileLogElement & element) override;
    
    std::vector<ProcessorProfileLogElement> getStoreResult() const {return store_vector;}
    std::vector<ProcessorProfileLogElement> store_vector;
};

MockConsumer::~MockConsumer() = default;;


void MockConsumer::consume(ProcessorProfileLogElement & element)
{
    store_vector.emplace_back(element);
}

TEST(ProfileLogHub, LogElementConsumeTest)
{
    ProfileLogHub<ProcessorProfileLogElement> profile_log_hub;
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> consumer = std::make_shared<MockConsumer>("test_query_id");
    profile_log_hub.initLogChannel("test_query_id", consumer);

    ThreadPool pool(1);
    pool.scheduleOrThrowOnError([&profile_log_hub]() {
        for (int i = 0; i < 10; i++) 
        {
            ProcessorProfileLogElement element;
            profile_log_hub.tryPushElement("test_query_id", element, 1);
        } 
        
    });
    
    pool.wait();
    profile_log_hub.stopConsume("test_query_id");
    while (!consumer->isFinish())
    {
        sleep(1);
    }
    
    auto result = dynamic_pointer_cast<MockConsumer>(consumer)->getStoreResult();
    ASSERT_EQ(result.size(), 10);
}
}
