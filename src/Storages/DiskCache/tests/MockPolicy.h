#pragma once

#include <algorithm>
#include <vector>

#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/FifoPolicy.h>
#include <common/types.h>

namespace DB::HybridCache
{
class MockPolicy : public EvictionPolicy
{
public:
    explicit MockPolicy(std::vector<UInt32> * hits_) : hits{*hits_}
    {
        using testing::_;
        using testing::Invoke;
        using testing::Return;


        ON_CALL(*this, track(_)).WillByDefault(Invoke([this](const Region & region) { policy.track(region); }));
        ON_CALL(*this, touch(_)).WillByDefault(Invoke([this](RegionId rid) {
            hits[rid.index()]++;
            policy.touch(rid);
        }));
        ON_CALL(*this, evict()).WillByDefault(Invoke([this] { return policy.evict(); }));
        ON_CALL(*this, reset()).WillByDefault(Invoke([this]() {
            std::fill(hits.begin(), hits.end(), 0);
            policy.reset();
        }));
        ON_CALL(*this, memorySize()).WillByDefault(Invoke([this] { return policy.memorySize(); }));
    }

    MOCK_METHOD1(track, void(const Region & region));
    MOCK_METHOD1(touch, void(RegionId rid));
    MOCK_METHOD0(evict, RegionId());
    MOCK_METHOD0(reset, void());
    MOCK_CONST_METHOD0(memorySize, size_t());

private:
    std::vector<UInt32> & hits;
    FifoPolicy policy;
};

MATCHER_P(EqRegionPri, pri, "region should have the same pri")
{
    return arg.getPriority() == static_cast<UInt32>(pri);
}

MATCHER_P(EqRegion, rid, "region should have the same id")
{
    return arg.id().index() == rid.index();
}

MATCHER_P2(EqRegion, rid, pri, "region should have the same id and pri")
{
    return arg.id().index() == rid.index() && arg.getPriority() == static_cast<UInt32>(pri);
}

inline void expectRegionsTracked(MockPolicy & policy, std::vector<UInt32> regionIds, bool sticky = true)
{
    testing::InSequence s;
    for (auto id : regionIds)
        EXPECT_CALL(policy, track(EqRegion(RegionId{id}))).RetiresOnSaturation();

    if (sticky)
        EXPECT_CALL(policy, track(testing::_)).Times(0);
    else
        EXPECT_CALL(policy, track(testing::_)).Times(testing::AtLeast(0));
}
}
