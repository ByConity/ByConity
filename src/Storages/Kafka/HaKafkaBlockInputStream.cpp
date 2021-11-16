#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/HaKafkaBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/Kafka/HaReadBufferFromKafkaConsumer.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Interpreters/addMissingDefaults.h>
#include <Storages/Kafka/KafkaConsumeInfo.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Processors/Formats/IRowInputFormat.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_AVAILABLE_CONSUMER;
}

/// sorted
Names HaKafkaBlockInputStream::default_virtual_column_names = {"_content", "_info", "_key", "_offset", "_partition", "_timestamp", "_topic"};

HaKafkaBlockInputStream::HaKafkaBlockInputStream(
    StorageHaKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::shared_ptr<Context> & context_,
    const Names & column_names_,
    size_t max_block_size_,
    size_t consumer_index_,
    bool need_materialized_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(column_names_)
    , max_block_size(max_block_size_)
    , consumer_index(consumer_index_)
    , need_materialized(need_materialized_)
    , virtual_column_names(storage.filterVirtualNames(column_names))
    , used_column(default_virtual_column_names.size(), -1)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , virtual_header(metadata_snapshot->getSampleBlockForColumns(virtual_column_names, storage.getVirtuals(), storage.getStorageID()))
{
    for (size_t i = 0, index = 0; i < default_virtual_column_names.size() && index < virtual_column_names.size(); ++i)
    {
        if (default_virtual_column_names[i] == virtual_column_names[index])
            used_column[i] = index++;
    }
    virtual_columns = virtual_header.cloneEmptyColumns();
}

HaKafkaBlockInputStream::~HaKafkaBlockInputStream()
{
    if (!delimited_buffer)
        return;

    writeKafkaLog();

    if (broken)
    {
        try
        {
            storage.unsubscribeBuffer(delimited_buffer);
        }
        catch (...)
        {
            LOG_ERROR(storage.log, "{}(): {}", __func__, getCurrentExceptionMessage(false));
        }
    }

    storage.pushBuffer(consumer_index);
}

void HaKafkaBlockInputStream::writeKafkaLog()
{
    try
    {
        /// Some metrics to system.kafka_log
        if (auto kafka_log = context->getKafkaLog())
        {
            auto read_buf = delimited_buffer->subBufferAs<HaReadBufferFromKafkaConsumer>();
            auto create_time = read_buf->getCreateTime();
            auto duration_ms = (read_buf->getAliveTime() - create_time) * 1000;

            /// Always write POLL log event if polled nothing
            auto kafka_poll_log = storage.createKafkaLog(KafkaLogElement::POLL, consumer_index);
            kafka_poll_log.event_time = create_time;
            kafka_poll_log.duration_ms = duration_ms;
            kafka_poll_log.metric = read_buf->getReadMessages();
            kafka_poll_log.bytes = read_buf->getReadBytes();
            kafka_log->add(kafka_poll_log);

            if (read_buf->getEmptyMessages() > 0)
            {
                auto kafka_empty_log = storage.createKafkaLog(KafkaLogElement::EMPTY_MESSAGE, consumer_index);
                kafka_empty_log.event_time = create_time;
                kafka_empty_log.duration_ms = duration_ms;
                kafka_empty_log.metric = read_buf->getEmptyMessages();
                kafka_log->add(kafka_empty_log);
            }

             IRowInputFormat * row_input = nullptr;
             if (children.size() > 0)
             {
                 if (auto input_stream = dynamic_cast<InputStreamFromInputFormat *>(children.back().get()))
                     row_input = dynamic_cast<IRowInputFormat *>(input_stream->getInputFormatPtr().get());
             }

            if (row_input && row_input->getNumErrors() > 0)
            {
                auto kafka_parse_log = storage.createKafkaLog(KafkaLogElement::PARSE_ERROR, consumer_index);
                kafka_parse_log.event_time = create_time;
                kafka_parse_log.duration_ms = duration_ms;
                kafka_parse_log.last_exception = row_input->getAndParseException().displayText();
                kafka_parse_log.metric = row_input->getNumErrors();
                kafka_parse_log.bytes = row_input->getErrorBytes();
                kafka_parse_log.has_error = true;
                kafka_log->add(kafka_parse_log);
            }
        }
    }
    catch (...)
    {
        LOG_ERROR(storage.log, "{}(): {}", __func__, getCurrentExceptionMessage(false));
    }
}

Block HaKafkaBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}

void HaKafkaBlockInputStream::readPrefixImpl()
{
    delimited_buffer = storage.tryClaimBuffer(consumer_index, context->getSettingsRef().queue_max_wait_ms.totalMilliseconds());

    /// 1. The streamThread catch this exception, it will retry at next time;
    /// 2. `SELECT FROM` HaKafka table, the user will receive this exception.
    if (!delimited_buffer)
        throw Exception("Failed to claim consumer!", ErrorCodes::NO_AVAILABLE_CONSUMER);

    storage.subscribeBuffer(delimited_buffer, consumer_index);

    const auto *sub_buffer = delimited_buffer->subBufferAs<HaReadBufferFromKafkaConsumer>();

    FormatFactory::ReadCallback read_callback;
    if (!virtual_column_names.empty())
    {
        read_callback = [this, sub_buffer]
        {
            if (used_column[0] >= 0) virtual_columns[used_column[0]]->insert(sub_buffer->currentContent());
            if (used_column[1] >= 0) virtual_columns[used_column[1]]->insertDefault();
            if (used_column[2] >= 0) virtual_columns[used_column[2]]->insert(sub_buffer->currentKey());
            if (used_column[3] >= 0) virtual_columns[used_column[3]]->insert(sub_buffer->currentOffset());
            if (used_column[4] >= 0) virtual_columns[used_column[4]]->insert(sub_buffer->currentPartition());
            if (used_column[5] >= 0) virtual_columns[used_column[5]]->insert(sub_buffer->currentTimeStamp());
            if (used_column[6] >= 0) virtual_columns[used_column[6]]->insert(sub_buffer->currentTopic());
        };
    }

    /// TODO: `input_format_parallel_parsing` is not supported for this logic
    auto input_format = FormatFactory::instance().getInput(storage.settings.format.value, *delimited_buffer, non_virtual_header,
            context, max_block_size /*, storage.settings.max_block_bytes_size.value, rows_portion_size, read_callback*/);

    if (auto row_input = dynamic_cast<IRowInputFormat*>(input_format.get()))
    {
        row_input->setReadCallBack(read_callback);
        row_input->setCallbackOnError([sub_buffer](Exception & e) {
            if (sub_buffer->currentMessage())
            {
                e.addMessage(
                    "\ntopic:     " + sub_buffer->currentTopic() + //
                    "\npartition: " + toString(sub_buffer->currentPartition()) + //
                    "\noffset:    " + toString(sub_buffer->currentOffset()) + //
                    "\ncontent:   " + sub_buffer->currentContent() //
                );
            }
        });
    }
    else
        throw Exception("An input format based on IRowInputFormat is expected, but provided: " + input_format->getName(), ErrorCodes::LOGICAL_ERROR);

    BlockInputStreamPtr stream = std::make_shared<InputStreamFromInputFormat>(std::move(input_format));
    addChild(stream);

    broken = true;
}

Block HaKafkaBlockInputStream::readImpl()
{
    /// original HaKafkaBlockInputStream
    Block block = children.back()->read();
    if (!block)
        return block;

    Block virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));
    virtual_columns = virtual_header.cloneEmptyColumns();

    for (const auto & column :  virtual_block.getColumnsWithTypeAndName())
        block.insert(column);

    /// Construct column with KafkaConsumeInfo in first row and other row with default value
    if (storage.enableMemoryTable())
    {
        auto read_buf = delimited_buffer->subBufferAs<HaReadBufferFromKafkaConsumer>();
        if (read_buf->getReadMessages() != 0)
        {
            /// Remove original info column replace with new column
            auto tpl = read_buf->getOffsets();
            KafkaConsumeInfo consume_info(tpl, consumer_index);
            String consume_info_str = consume_info.toString();
            convertBlock<DataTypeString>(block, "_info", consume_info_str);
        }
    }

    /// TODO: support default value here
    if (need_materialized)
    ///     block = addMissingDefaults(block, getHeader().getNamesAndTypesList(), metadata_snapshot->getColumns().getDefaults(), context);
        LOG_WARNING(storage.log, "Noting todo for `need_materialized` in HaKafkaBlockInputStream");

    return ConvertingBlockInputStream(
            std::make_shared<OneBlockInputStream>(block), getHeader(),
            ConvertingBlockInputStream::MatchColumnsMode::Name).read();
}

void HaKafkaBlockInputStream::readSuffixImpl()
{
    broken = false;
}

void HaKafkaBlockInputStream::forceCommit()
{
    delimited_buffer->subBufferAs<HaReadBufferFromKafkaConsumer>()->commit();
}

}

#endif
