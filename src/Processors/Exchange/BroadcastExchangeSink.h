/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <Common/Logger.h>
#include <atomic>
#include <Processors/Exchange/BufferChunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/IProcessor.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>

namespace DB
{

/// Sink which broadcast data to ExchangeSource.
class BroadcastExchangeSink : public IExchangeSink
{
public:
    BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_, ExchangeOptions options_, const String &name_);
    virtual ~BroadcastExchangeSink() override;
    String getName() const override { return name; }
    BroadcastSenderPtrs getSenders() const
    {
        return senders;
    }

    static String generateName(size_t exchange_id)
    {
        return fmt::format("BroadcastExchangeSink[{}]", exchange_id);
    }

    static String generateNameForTest()
    {
        return fmt::format("BroadcastExchangeSink[{}]", -1);
    }

protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;
    virtual void onCancel() override;

private:
    String name;
    BroadcastSenderPtrs senders;
    ExchangeOptions options;
    BufferChunk buffer_chunk;
    LoggerPtr logger;
};

}
