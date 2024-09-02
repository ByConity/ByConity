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

#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/CnchReadBufferFromKafkaConsumer.h>
#include <Storages/Kafka/KafkaCommon.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int RDKAFKA_EXCEPTION;
    }

using namespace std::chrono_literals;

const auto DRAIN_TIMEOUT_MS = 5000ms;

CnchReadBufferFromKafkaConsumer::~CnchReadBufferFromKafkaConsumer()
{
    try
    {
        if (current)
            current = Message();

        /// NOTE: see https://github.com/edenhill/librdkafka/issues/2077
        consumer->unsubscribe();
        consumer->unassign();
    }
    catch (...)
    {
        LOG_ERROR(log, "{}(): {}", __func__, getCurrentExceptionMessage(false));
    }

    try
    {
        drain();
    }
    catch (...)
    {
        LOG_ERROR(log, "{}(): {}", __func__, getCurrentExceptionMessage(false));
    }
}

// Needed to drain rest of the messages / queued callback calls from the consumer
// after unsubscribe, otherwise consumer will hang on destruction
// see https://github.com/edenhill/librdkafka/issues/2077
//     https://github.com/confluentinc/confluent-kafka-go/issues/189 etc.
void CnchReadBufferFromKafkaConsumer::drain()
{
    auto start_time = std::chrono::steady_clock::now();
    cppkafka::Error last_error(RD_KAFKA_RESP_ERR_NO_ERROR);

    while (true)
    {
        auto msg = consumer->poll(100ms);
        if (!msg)
            break;

        auto error = msg.get_error();

        if (error)
        {
            if (msg.is_eof() || error == last_error)
            {
                break;
            }
            else
            {
                LOG_ERROR(log, "Error during draining: {}", error.to_string());
            }
        }

        // i don't stop draining on first error,
        // only if it repeats once again sequentially
        last_error = error;

        auto ts = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time) > DRAIN_TIMEOUT_MS)
        {
            LOG_ERROR(log, "Timeout during draining.");
            break;
        }
    }
}

cppkafka::TopicPartitionList CnchReadBufferFromKafkaConsumer::getOffsets() const
{
    cppkafka::TopicPartitionList tpl;
    for (const auto & p : offsets)
        tpl.emplace_back(p.first.first, p.first.second, p.second);
    return tpl;
}

void CnchReadBufferFromKafkaConsumer::clearOffsets()
{
    offsets.clear();
}

void CnchReadBufferFromKafkaConsumer::commit()
{
    if (!offsets.empty())
    {
        auto tpl = getOffsets();
        LOG_DEBUG(log, "Committing offsets: {}", DB::Kafka::toString(tpl));

        try
        {
            consumer->commit(tpl);
            /// Clear committed offsets to avoid redundant committing
            offsets.clear();
        }
        catch (...)
        {
            /// TODO P0:
            /// Ignore commit exception or the messages would be duplicated
            /// The offsets are kept for next committing
            LOG_ERROR(log, "{}(): {}", __func__, getCurrentExceptionMessage(false));
        }
    }

    /// Still reset current message to free resource
    if (current)
        current = Message();
}

void CnchReadBufferFromKafkaConsumer::subscribe(const Names & topics)
{
    // While we wait for an assignment after subscribtion, we'll poll zero messages anyway.
    // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
    if (consumer->get_cached_subscription().empty())
    {
        consumer->pause(); // don't accidentally read any messages
        consumer->subscribe(topics);
        consumer->poll(2s);
        consumer->resume();
    }

    reset();
}

void CnchReadBufferFromKafkaConsumer::unsubscribe()
{
    LOG_TRACE(log, "Re-joining claimed consumer after failure");
    /// Need clear recorded offsets to avoid messages loss
    offsets.clear();
    consumer->unsubscribe();

    drain();
}

void CnchReadBufferFromKafkaConsumer::assign(const cppkafka::TopicPartitionList & topic_partition_list)
{
    if (consumer->get_cached_assignment().empty())
        consumer->assign(topic_partition_list);

    reset();
}

void CnchReadBufferFromKafkaConsumer::setSampleConsumingPartitionList(const std::set<cppkafka::TopicPartition> & sample_partitions_)
{
    if (!sample_partitions_.empty())
    {
        sample_partitions = sample_partitions_;
        enable_sample_consuming = true;
    }
}

void CnchReadBufferFromKafkaConsumer::unassign()
{
    LOG_TRACE(log, "Re-joining claimed consumer after failure");
    /// Need clear recorded offsets to avoid messages loss
    offsets.clear();
    consumer->unassign();

    drain();
}


bool CnchReadBufferFromKafkaConsumer::nextImpl()
{
    /// NOTE: ReadBuffer was implemented with an immutable underlying contents in mind.
    ///       If we failed to poll any message once - don't try again.
    ///       Otherwise, the |poll_timeout| expectations get flawn.
    if (stalled)
        return false;

    while (!hasExpired() && read_messages < batch_size && run->load(std::memory_order_relaxed))
    {
        /// the api `poll` will check the error if it gets a message, and it will throw an exception if there is an error;
        /// thus here we don't need to check if the message has some error;
        /// Of course, we may get no message, e.g there are no more messages in the topic-partition now.
        auto new_message = consumer->poll(std::chrono::milliseconds(poll_timeout));
        if (!new_message || new_message.is_eof())
            continue;

        /// Must continue polling and handling the consumer queue even if the queue is filled with errors;
        /// or the memory leak occurs because the consumer keeps fetching from brokers and fill the queue
        if (auto error = new_message.get_error())
        {
            ++rdkafka_errors;
            if (consumer->is_serious_err(error))
                consumer->setDestroyed();

            rdkafka_errors_buffer.push_back({"poll(): " + error.to_string(), static_cast<UInt64>(Poco::Timestamp().epochTime())});
            continue;
        }

        /// Get an available message, save it for committing
        current = std::move(new_message);
        read_messages += 1;

        auto & offset = offsets[{current.get_topic(), current.get_partition()}];
        if (offset > 0 && current.get_offset() != offset)
        {
            if (current.get_offset() > offset)
            {
                String msg = "Poll skipped message in " + current.get_topic() + '#' + std::to_string(current.get_partition())
                             + ": expected " + std::to_string(offset) + " but got " + std::to_string(current.get_offset());
                if (enable_skip_offsets_hole)
                {
                    /// It seems the offsets hole produced by kafka producer can not be skipped by `auto.reset.offset` policy
                    ///  as it does not belong to 'out of range' exception;
                    /// So we need to hand it specially if you indeed produce a topic with offsets holes
                    LOG_WARNING(log, msg + ". We will skip this hole as you have enabled `enable_skip_offsets_hole`");

                    skipped_msgs_in_holes += (current.get_offset() - offset);
                    skipped_ofsets_hole.emplace_back(current.get_topic() + "#" + std::to_string(current.get_partition())
                                                     + ": [" + std::to_string(offset) + ", " + std::to_string(current.get_offset()) + ")");
                }
                else
                    throw Exception(
                        msg + ". This may be caused by kafka retention policy with a long time lag",
                        ErrorCodes::RDKAFKA_EXCEPTION);
            }
            else if (current.get_offset() < offset)
                LOG_WARNING(
                    log,
                    "Poll duplicated message in {}#{}: : expected {} but got {}",
                    current.get_topic(), current.get_partition(), offset, current.get_offset());
        }

        /// The term `position` gives the offset of the next message (i.e. offset of current message + 1)
        /// Record or update this `position` for later committing,
        /// Once committed, the `postition` and the `committed position` would be equal
        offset = current.get_offset() + 1;

        /// if sample consuming is enabled and the consumed topic & partition is not the sampled one,
        /// the message would be discarded; but we still record the offset to avoid the lag
        if (unlikely(enable_sample_consuming) && !sample_partitions.contains({current.get_topic(), current.get_partition()}))
        {
            /// XXX: record the discarded message number here
            ++skip_messages_by_sample;
            continue;
        }

        const auto & payload = current.get_payload();

        /// Check empty payload to avoid CORE DUMP
        if (!payload || !payload.get_data())
        {
            empty_messages += 1;
            continue;
        }

        read_bytes += payload.get_size();

        /// Here, we got a new message

        // XXX: very fishy place with const casting.
        auto *new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(payload.get_data()));
        BufferBase::set(new_position, payload.get_size(), 0);
        return true;
    }

    /// This buffer/consumer has been expired if reached here
    LOG_DEBUG(log, "Stalled. Polled {} messages and {} errors", read_messages, rdkafka_errors);
    stalled = true;
    return false;
}


void CnchReadBufferFromKafkaConsumer::reset()
{
    create_time = time(nullptr);
    read_messages = 0;
    read_bytes = 0;
    empty_messages = 0;
    stalled = false;
    skipped_msgs_in_holes = 0;
    skipped_ofsets_hole.clear();
    skip_messages_by_sample = 0;
    rdkafka_errors = 0;
    rdkafka_errors_buffer.clear();
}

bool CnchReadBufferFromKafkaConsumer::hasExpired()
{
    alive_time = time(nullptr);
    return ((alive_time - create_time) * 1000 > expire_timeout);
}

}

#endif
