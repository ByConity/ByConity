#include <random>
#include <thread>

#include <gtest/gtest.h>

#include <Storages/DiskCache/Contexts.h>
#include <Storages/DiskCache/InFlightPuts.h>
#include <common/StringRef.h>

namespace DB
{
TEST(InFlightPutsTest, FunctionExecution)
{
    InFlightPuts p;
    StringRef key = "foobar";
    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());

    bool executed = false;
    auto fn = [&]() { executed = true; };
    auto res = token.executeIfValid(fn);
    ASSERT_EQ(res, executed);
    ASSERT_FALSE(token.isValid());

    executed = false;
    ASSERT_FALSE(token.executeIfValid(fn));
    ASSERT_FALSE(executed);
}

TEST(InFlightPutsTest, TokenMove)
{
    InFlightPuts p;
    StringRef key = "foobar";
    {
        auto token = p.tryAcquireToken(key);
        ASSERT_TRUE(token.isValid());
        auto moved_token = std::move(token);

        ASSERT_FALSE(token.isValid());
        ASSERT_TRUE(moved_token.isValid());

        InFlightPuts::PutToken move_assign_token{};
        ASSERT_FALSE(move_assign_token.isValid());
        move_assign_token = std::move(moved_token);
        ASSERT_FALSE(moved_token.isValid());
        ASSERT_TRUE(move_assign_token.isValid());
    }
    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());
}

TEST(InFlightPutsTest, FunctionException)
{
    InFlightPuts p;
    StringRef key = "foobar";
    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());

    bool executed = false;
    auto throw_fn = []() { throw std::runtime_error(""); };
    ASSERT_THROW(token.executeIfValid(throw_fn), std::runtime_error);
    ASSERT_FALSE(executed);
    ASSERT_TRUE(token.isValid());

    auto fn = [&]() { executed = true; };

    auto res = token.executeIfValid(fn);
    ASSERT_EQ(res, executed);
    ASSERT_TRUE(executed);
    ASSERT_FALSE(token.isValid());
}

TEST(InFlightPutsTest, Collision)
{
    InFlightPuts p;
    StringRef key = "foobar";
    {
        auto token = p.tryAcquireToken(key);
        ASSERT_TRUE(token.isValid());

        auto token2 = p.tryAcquireToken(key);
        ASSERT_FALSE(token2.isValid());
    }

    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());
}

TEST(InFlightPutsTest, InvalidationSimple)
{
    InFlightPuts p;
    StringRef key = "foobar";

    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());

    p.invalidateToken(key);
    ASSERT_TRUE(token.isValid());

    bool executed = false;
    auto fn = [&]() { executed = true; };
    ASSERT_FALSE(token.executeIfValid(fn));
    ASSERT_FALSE(executed);

    ASSERT_FALSE(p.tryAcquireToken(key).isValid());
}

TEST(InFlightPutsTest, InvalidationAndCreate)
{
    InFlightPuts p;
    StringRef key = "foobar";

    bool executed = false;
    auto fn = [&]() { executed = true; };
    {
        auto token = p.tryAcquireToken(key);
        ASSERT_TRUE(token.isValid());

        p.invalidateToken(key);
        ASSERT_TRUE(token.isValid());

        auto new_token = p.tryAcquireToken(key);
        ASSERT_FALSE(new_token.isValid());

        ASSERT_FALSE(token.executeIfValid(fn));
        ASSERT_FALSE(executed);
    }

    auto token = p.tryAcquireToken(key);
    ASSERT_TRUE(token.isValid());
    executed = false;
    ASSERT_TRUE(token.executeIfValid(fn));
    ASSERT_TRUE(executed);
}

void runInThreads(const std::function<void(int index)> & f, unsigned int nThreads)
{
    std::vector<std::thread> threads;
    for (unsigned int i = 0; i < nThreads; i++)
        threads.emplace_back(f, i);

    for (auto & t : threads)
    {
        if (t.joinable())
            t.join();
    }
}

TEST(TombStoneTest, ConcurrentAddRemove)
{
    TombStones t;
    const unsigned int n_threads = 10;
    const unsigned int n_hashes = 100;
    std::vector<std::string> hashes;
    for (unsigned int i = 0; i < n_hashes; i++)
        hashes.push_back(std::to_string(i));

    std::vector<std::vector<std::unique_ptr<TombStones::Guard>>> guards;
    for (unsigned int i = 0; i < n_threads; i++)
        guards.emplace_back();

    auto add_func = [&t, hashes, &guards](int index) mutable {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(hashes.begin(), hashes.end(), g);
        for (const auto & hash : hashes)
            guards[index].push_back(std::make_unique<TombStones::Guard>(t.add(hash)));
    };

    runInThreads(add_func, n_threads);
    for (const auto & hash : hashes)
        ASSERT_TRUE(t.isPresent(hash));

    auto remove_func = [&guards](int index) mutable {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(guards[index].begin(), guards[index].end(), g);
        for (auto & guard : guards[index])
            guard.reset();
    };

    runInThreads(remove_func, n_threads);
    for (const auto & hash : hashes)
        ASSERT_FALSE(t.isPresent(hash));
}

TEST(TombStoneTest, MultipleGuard)
{
    TombStones t;
    int n_guards = 10;
    std::string key = "12325";
    std::vector<std::unique_ptr<TombStones::Guard>> guards(n_guards);
    for (int i = 0; i < n_guards; i++)
        guards[i] = std::make_unique<TombStones::Guard>(t.add(key));

    ASSERT_TRUE(t.isPresent(key));

    for (int i = 0; i < n_guards - 1; i++)
    {
        guards[i].reset();
        ASSERT_TRUE(t.isPresent(key));
    }
    guards[n_guards - 1].reset();
    ASSERT_FALSE(t.isPresent(key));
}

TEST(TombStoneTest, Move)
{
    TombStones t;
    std::string key = "12325";
    {
        auto guard = t.add(key);
        ASSERT_TRUE(t.isPresent(key));
        auto move_guard = std::move(guard);
        ASSERT_TRUE(t.isPresent(key));
        TombStones::Guard move_assign_guard;
        move_assign_guard = std::move(move_guard);
        ASSERT_TRUE(t.isPresent(key));
    }
    ASSERT_FALSE(t.isPresent(key));
}

}
