#pragma once
#include <atomic>
#include <Processors/ISink.h>
#include <Poco/Logger.h>

namespace DB
{
/// Base Sink which transfer chunk to ExchangeSource.
class IExchangeSink : public ISink
{
public:
    explicit IExchangeSink(Block header);
    Status prepare() override;

protected:
    void finish();

private:
    std::atomic_bool is_finished{false};
};

}
