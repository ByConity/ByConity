#include <UDF/client/IClient.h>
#include <UDF/client/IClient.cpp>
#include <UDF/client/LoadBalancer.h>
#include <gtest/gtest.h>
#include <list>
#include <thread>

using namespace DB::UDF;
#define MAX_UDF_SERVERS (512)

class TestClient : public IClient {
public:
    int GetOffset(void) const override {
        return id;
    }

    virtual void ScalarCall(brpc::Controller *, const ScalarReq *) override {};

    virtual void AggregateCall(brpc::Controller *, const AggregateReq *) override {};

    virtual void OnFatalError() override {}

    TestClient(int id_) {
        id = id_;
    }

private:
    int id;
};

static std::vector<std::unique_ptr<IClient>> create_dummyclients(int cnt)
{
    std::vector<std::unique_ptr<IClient>> clients;

    for (int id = 0; id < cnt; id++)
        clients.emplace_back(std::make_unique<TestClient>(id));

    return clients;
}

TEST(UDFLoadBalancer, server_size)
{
    for (int s = 1; s <= 64; s++) {
        LoadBalancer lb(create_dummyclients(s));
        std::list<IClient *> clients;
        int i;

        EXPECT_EQ(lb.FindFirstFreeServerIndex(), 0);
        EXPECT_EQ(lb.GetFreeCount(), s);
        for (i = 0; i < s; i++) {
            EXPECT_EQ(lb.FindFirstFreeServerIndex(), i);
            IClient *client = lb.SelectServer();

            EXPECT_EQ(client->GetOffset(), i);
            clients.push_back(client);
            EXPECT_EQ(lb.GetFreeCount(), s - i - 1);
        }

        EXPECT_EQ(lb.FindFirstFreeServerIndex(), -1);
        EXPECT_EQ(lb.GetFreeCount(), 0);
        while (clients.size()) {
            lb.Feedback(clients.back());
            clients.pop_back();
            EXPECT_EQ(lb.FindFirstFreeServerIndex(), --i);
            EXPECT_EQ(lb.GetFreeCount(), s - i);
        }
    }
}

TEST(UDFLoadBalancer, single_thread_seqential_access)
{
    LoadBalancer lb(create_dummyclients(MAX_UDF_SERVERS));
    std::list<IClient *> clients;
    int i;

    EXPECT_EQ(lb.FindFirstFreeServerIndex(), 0);
    EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS);
    for (i = 0; i < MAX_UDF_SERVERS; i++) {
        EXPECT_EQ(lb.FindFirstFreeServerIndex(), i);
        IClient *client = lb.SelectServer();

        EXPECT_EQ(client->GetOffset(), i);
        clients.push_back(client);
        EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS - i - 1);
    }

    EXPECT_EQ(lb.FindFirstFreeServerIndex(), -1);
    EXPECT_EQ(lb.GetFreeCount(), 0);
    while (clients.size()) {
        lb.Feedback(clients.back());
        clients.pop_back();
        EXPECT_EQ(lb.FindFirstFreeServerIndex(), --i);
        EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS - i);
    }

    EXPECT_EQ(lb.FindFirstFreeServerIndex(), 0);
    EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS);
    for (i = 0; i < MAX_UDF_SERVERS; i++)
        clients.push_back(lb.SelectServer());

    i = 0;
    EXPECT_EQ(lb.FindFirstFreeServerIndex(), -1);
    EXPECT_EQ(lb.GetFreeCount(), 0);
    while (clients.size()) {
        lb.Feedback(clients.front());
        clients.pop_front();
        EXPECT_EQ(lb.GetFreeCount(), ++i);
    }
}

TEST(UDFLoadBalancer, concurrent_access)
{
    LoadBalancer lb(create_dummyclients(MAX_UDF_SERVERS));
    std::vector<std::thread> threads;
    std::list<IClient *> clients;
    int i;

#define MAX_THREADS (256)
    EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS);
    for (i = MAX_THREADS; i > 0; i--) {
        threads.push_back(std::thread([&lb, &i]() {
            for (int l = 0; l <= 4096 * i; l++)
                lb.Feedback(lb.SelectServer());
        }));
    }

    for (auto &t : threads)
        t.join();

    EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS);
    EXPECT_EQ(lb.FindFirstFreeServerIndex(), 0);
    for (i = 0; i < MAX_UDF_SERVERS; i++) {
        EXPECT_EQ(lb.FindFirstFreeServerIndex(), i);
        clients.push_back(lb.SelectServer());
        EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS - i - 1);
    }

    EXPECT_EQ(lb.FindFirstFreeServerIndex(), -1);
    EXPECT_EQ(lb.GetFreeCount(), 0);
    while (clients.size()) {
        lb.Feedback(clients.back());
        clients.pop_back();
        EXPECT_EQ(lb.FindFirstFreeServerIndex(), --i);
        EXPECT_EQ(lb.GetFreeCount(), MAX_UDF_SERVERS - i);
    }
}
