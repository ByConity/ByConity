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
        ON_CALL(*this, persist(_)).WillByDefault(Invoke([this](google::protobuf::io::CodedOutputStream * stream) {
            policy.persist(stream);
        }));
        ON_CALL(*this, recover(_)).WillByDefault(Invoke([this](google::protobuf::io::CodedInputStream * stream) {
            policy.recover(stream);
        }));
    }

    MOCK_METHOD1(track, void(const Region & region));
    MOCK_METHOD1(touch, void(RegionId rid));
    MOCK_METHOD0(evict, RegionId());
    MOCK_METHOD0(reset, void());
    MOCK_CONST_METHOD0(memorySize, size_t());

    MOCK_CONST_METHOD1(persist, void(google::protobuf::io::CodedOutputStream *));
    MOCK_METHOD1(recover, void(google::protobuf::io::CodedInputStream *));

private:
    std::vector<UInt32> & hits;
    FifoPolicy policy;
};

MATCHER_P(EqRegionPri, pri, "region should have the same pri")
{
    return arg.getPriority() == static_cast<UInt32>(pri);
}
}
