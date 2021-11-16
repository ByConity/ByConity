#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/HaReadBufferFromKafkaConsumer.h>

namespace DB
{

namespace ErrorCodes
{
extern const int RDKAFKA_EXCEPTION;
}

using namespace std::chrono_literals;

HaReadBufferFromKafkaConsumer::~HaReadBufferFromKafkaConsumer()
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
        LOG_ERROR(log, "{}(): ", __func__, getCurrentExceptionMessage(false));
    }

    try
    {
        while (consumer->get_consumer_queue().next_event(50ms));
    }
    catch (...)
    {
        LOG_ERROR(log, "{}(): ", __func__, getCurrentExceptionMessage(false));
    }
}

cppkafka::TopicPartitionList HaReadBufferFromKafkaConsumer::getOffsets() const
{
    cppkafka::TopicPartitionList tpl;
    for (auto & p : offsets)
        tpl.emplace_back(p.first.first, p.first.second, p.second);
    return tpl;
}

void HaReadBufferFromKafkaConsumer::commit()
{
    if (!offsets.empty())
    {
        auto tpl = getOffsets();
        try
        {
            /// FIXME: `tpl` cannot be added into log with fmtlib in LOG
            /// LOG_DEBUG(log, "Committing offsets: {}", tpl);

            /// auto positions = consumer->get_offsets_position(tpl);
            /// LOG_DEBUG(log, "Current positions: " << positions);

            consumer->commit(tpl);
        }
        catch (...)
        {
            /// TODO P0:
            /// Ignore commit exception or the messages would be duplicated
            /// The offsets are kept for next committing
            LOG_ERROR(log, "{}(): ", __func__, getCurrentExceptionMessage(false));
        }
    }

    /// Still reset current message to free resource
    if (current)
        current = Message();
}

void HaReadBufferFromKafkaConsumer::subscribe(const Names & topics)
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

void HaReadBufferFromKafkaConsumer::unsubscribe()
{
    LOG_DEBUG(log, "Re-joining claimed consumer after failure");

    /// There may be another consumer thread working when `enable_poll_in_pipeline`
    cancelled = true;
    std::unique_lock lock(cancel_mutex);
    cancel_cv.wait_for(lock, std::chrono::milliseconds(expire_timeout), [this] { return stalled || !run->load(std::memory_order_relaxed); });

    /// Need clear recorded offsets to avoid messages loss
    offsets.clear();
    consumer->unsubscribe();
}

void HaReadBufferFromKafkaConsumer::assign(const cppkafka::TopicPartitionList & topic_partition_list)
{
    if (consumer->get_cached_assignment().empty())
    {
        /// LOG_DEBUG(log, "assign {}", topic_partition_list);
        consumer->assign(topic_partition_list);
    }

    reset();
}

void HaReadBufferFromKafkaConsumer::unassign()
{
    LOG_DEBUG(log, "Re-joining claimed consumer after failure");

    /// There may be another consumer thread working when `enable_poll_in_pipeline`
    cancelled = true;
    std::unique_lock lock(cancel_mutex);
    cancel_cv.wait_for(lock, std::chrono::milliseconds(expire_timeout), [this] { return stalled || !run->load(std::memory_order_relaxed); });

    /// Need clear recorded offsets to avoid messages loss
    offsets.clear();
    consumer->unassign();
}


bool HaReadBufferFromKafkaConsumer::nextImpl()
{
    /// NOTE: ReadBuffer was implemented with an immutable underlying contents in mind.
    ///       If we failed to poll any message once - don't try again.
    ///       Otherwise, the |poll_timeout| expectations get flawn.
    if (stalled)
        return false;

    /// XXX: DO NOT limit batch size currently, which would affect thread pool scheduling.
    /// NOTE: That modification dose not slow down or speed up consuming.
    while (!hasExpired() && read_messages < batch_size
           && run->load(std::memory_order_relaxed)
           && !cancelled.load(std::memory_order_relaxed))
    {
        auto new_message = consumer->poll(std::chrono::milliseconds(poll_timeout));
        if (!new_message)
            continue;

        if (new_message.is_eof())
        {
            std::this_thread::sleep_for(100ms);
            continue;
        }

        /// Get an available message, save it for committing
        current = std::move(new_message);
        read_messages += 1;

        auto & offset = offsets[{current.get_topic(), current.get_partition()}];
        if (offset > 0 && current.get_offset() != offset)
        {
            if (current.get_offset() > offset)
                throw Exception(
                    "Poll skipped message in " + current.get_topic() + '#' + std::to_string(current.get_partition())
                        + ": expected " + std::to_string(offset) + " but got " + std::to_string(current.get_offset()),
                    ErrorCodes::RDKAFKA_EXCEPTION);
            else if (current.get_offset() < offset)
                LOG_WARNING(
                    log, "Poll duplicated message in {} # {}: expected {} but got {} ",
                    current.get_topic(), current.get_partition(), offset, current.get_offset());
        }

        /// The term `position` gives the offset of the next message (i.e. offset of current message + 1)
        /// Record or update this `position` for later committing,
        /// Once committed, the `postition` and the `committed position` would be equal
        offset = current.get_offset() + 1;

        auto & payload = current.get_payload();

        /// Check empty payload to avoid CORE DUMP
        if (!payload || !payload.get_data())
        {
            empty_messages += 1;
            continue;
        }

        read_bytes += payload.get_size();

        /// Here, we got a new message

        // XXX: very fishy place with const casting.
        auto new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(payload.get_data()));
        BufferBase::set(new_position, payload.get_size(), 0);
        return true;
    }

    /// This buffer/consumer has been expired if reached here
    LOG_TRACE(log, "Stalled. Polled {} messages", read_messages);
    {
        std::unique_lock lock(cancel_mutex);
        stalled = true;
        cancel_cv.notify_all();
    }
    return false;
}


void HaReadBufferFromKafkaConsumer::reset()
{
    create_time = time(nullptr);
    read_messages = 0;
    empty_messages = 0;
    stalled = false;
    cancelled = false;
    read_bytes = 0;
}

bool HaReadBufferFromKafkaConsumer::hasExpired()
{
    alive_time = time(nullptr);
    return ((alive_time - create_time) * 1000 > expire_timeout);
}

}

#endif
