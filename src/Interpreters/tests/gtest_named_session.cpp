#include <Interpreters/Context.h>
#include <Interpreters/NamedSession.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

TEST(NamedSessionsTest, AcquireAndReleaseSession)
{
    NamedCnchSessions sessions;

    auto context = Context::createCopy(getContext().context);
    auto session = sessions.acquireSession(1, context, 10, false, false);
    // 2 = shared_ptr of current session + that in sessions
    ASSERT_EQ(session.use_count(), 2);
    auto session2 = sessions.acquireSession(2, context, 30, false, false);
    auto session3 = sessions.acquireSession(3, context, 20, false, false);
    ASSERT_EQ(sessions.getCurrentActiveSession(), 3);

    session->release();
    ASSERT_EQ(sessions.getCurrentActiveSession(), 2);
    session2->release();
    session3->release();
    ASSERT_EQ(sessions.getCurrentActiveSession(), 0);
}

TEST(NamedSessionsTest, SessionContainerTest)
{
    NamedCnchSessions parent;
    NamedCnchSessions::SessionContainer sessions;

    auto context = Context::createCopy(getContext().context);
    sessions.insert(std::make_shared<NamedCnchSession>(1, context, 10, parent));
    sessions.insert(std::make_shared<NamedCnchSession>(2, context, 30, parent));
    sessions.insert(std::make_shared<NamedCnchSession>(3, context, 20, parent));
    ASSERT_EQ(sessions.size(), 3);

    // verify traverse by close time
    auto & sessions_by_close = sessions.template get<0>();
    auto session_iter = sessions_by_close.begin();
    ASSERT_EQ((*session_iter)->timeout, 10);
    session_iter++;
    ASSERT_EQ((*session_iter)->timeout, 20);
    // verify erase by iterator and the iterator position return by erase()
    session_iter = sessions_by_close.erase(session_iter);
    ASSERT_EQ((*session_iter)->timeout, 30);
    session_iter++;
    ASSERT_TRUE(session_iter == sessions_by_close.end());
    ASSERT_EQ(sessions.size(), 2);

    // verify find by session key
    auto & sessions_by_key = sessions.template get<1>();
    auto session_iter_2 = sessions_by_key.find(1);
    ASSERT_EQ((*session_iter_2)->timeout, 10);

    // verify erase by session key
    sessions_by_key.erase(1);
    ASSERT_EQ(sessions.size(), 1);
}

TEST(NamedSessionsTest, SessionContainerUpdateTest)
{
    NamedCnchSessions parent;
    NamedCnchSessions::SessionContainer sessions;

    auto context = Context::createCopy(getContext().context);
    sessions.insert(std::make_shared<NamedCnchSession>(1, context, 10, parent));
    sessions.insert(std::make_shared<NamedCnchSession>(2, context, 30, parent));
    sessions.insert(std::make_shared<NamedCnchSession>(3, context, 20, parent));

    // verify the sequence after updating close time
    size_t timeout = 100;
    auto & sessions_by_key = sessions.template get<1>();
    sessions_by_key.modify(sessions_by_key.find(1), [&timeout](auto & local_session) {
        local_session->timeout = timeout;
        local_session->close_time = Poco::Timestamp().epochTime() + timeout;
    });

    timeout = 50;
    sessions_by_key.modify(sessions_by_key.find(3), [&timeout](auto & local_session) {
        local_session->timeout = timeout;
        local_session->close_time = Poco::Timestamp().epochTime() + timeout;
    });

    auto & sessions_by_close = sessions.template get<0>();
    auto session_iter = sessions_by_close.begin();
    ASSERT_EQ((*session_iter)->timeout, 30);
    session_iter++;
    ASSERT_EQ((*session_iter)->timeout, 50);
    session_iter++;
    ASSERT_EQ((*session_iter)->timeout, 100);
    session_iter++;
    ASSERT_TRUE(session_iter == sessions_by_close.end());
}
