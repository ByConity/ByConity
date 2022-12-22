#pragma once
#include <Statistics/StatisticsBaseImpl.h>
#include <Common/Exception.h>
namespace DB::Statistics
{
class StatsDummyAlpha : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::DummyAlpha;
    StatsDummyAlpha() = default;
    StatsDummyAlpha(int num) : magic_num(num) { }

    String serialize() const override;
    void deserialize(std::string_view blob) override;
    StatisticsTag getTag() const override { return tag; }
    auto get() { return magic_num; }

private:
    int magic_num = 0;
};


class StatsDummyBeta : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::DummyBeta;
    StatsDummyBeta() = default;
    StatsDummyBeta(String str) : magic_str(std::move(str)) { }
    String serialize() const override;
    void deserialize(std::string_view blob) override;
    StatisticsTag getTag() const override { return tag; }
    auto get() { return magic_str; }

private:
    String magic_str;
};
}
