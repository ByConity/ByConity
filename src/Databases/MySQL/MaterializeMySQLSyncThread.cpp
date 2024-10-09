#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Databases/MySQL/DatabaseCloudMaterializedMySQL.h>
#    include <cstdlib>
#    include <random>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnDecimal.h>
#    include <DataStreams/CountingBlockOutputStream.h>
#    include <DataStreams/OneBlockInputStream.h>
#    include <DataStreams/copyData.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/executeQuery.h>
#    include <Interpreters/CnchSystemLog.h>
#    include <Storages/StorageMergeTree.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>
#    include <common/sleep.h>
#    include <common/bit_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_MYSQL_VARIABLE;
    extern const int SYNC_MYSQL_USER_ACCESS_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_ALL_DATA;
}

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

static BlockIO tryToExecuteQuery(const String & query_to_execute, ContextMutablePtr query_context, const String & database, const String & comment)
{
    try
    {
        if (!database.empty())
            query_context->setCurrentDatabase(database);

        return executeQuery("/*" + comment + "*/ " + query_to_execute, query_context, true);
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("MaterializeMySQLSyncThread(" + database + ")"),
            "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }
}

MaterializeMySQLSyncThread::~MaterializeMySQLSyncThread()
{
    try
    {
        stopSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

static String dumpPosition(const Position & position)
{
    WriteBufferFromOwnString buf;
    position.dump(buf);
    return buf.str();
}

MaterializeMySQLSyncThread::MaterializeMySQLSyncThread(
    ContextPtr context_,
    const String & database_name_,
    const String & mysql_database_name_,
    const String & thread_key_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    MaterializeMySQLSettings * settings_,
    const String & assigned_table)
    : WithContext(context_->getGlobalContext())
    , log(getLogger("MaterializedMySQLSyncThread (" + database_name_ + ") "))
    , database_name(database_name_)
    , mysql_database_name(mysql_database_name_)
    , assigned_materialized_table(assigned_table)
    , thread_key(thread_key_)
    , pool(std::move(pool_))
    , client(std::move(client_))
    , settings(settings_)
{
    query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(database_name) + ", " + backQuoteIfNeed(mysql_database_name) + ") ";
}

void MaterializeMySQLSyncThread::synchronization()
{
    setThreadName(MYSQL_BACKGROUND_THREAD_NAME);

    try
    {
        /// Before it will be read from persistent file each time; so if data is flushed, it will be updated
        MaterializeMetadata metadata(binlog_info, getContext()->getSettingsRef());
        bool need_reconnect = true;
        const auto thread_schedule_interval_ms = settings->sync_thread_schedule_interval_ms.value;
        UInt64 max_flush_time = settings->max_flush_data_time;

        Buffers buffers(database_name, thread_key);
        UInt64 sum{0}, last{0};
        bool got_event{false};
        Stopwatch watch;

        while (!isCancelled())
        {
            if (need_reconnect)
            {
                if (!prepareSynchronized(metadata))
                    break;
                need_reconnect = false;
            }

            try
            {
                got_event = false;
                BinlogEventPtr binlog_event = client.readOneBinlogEvent(std::max(UInt64(1), max_flush_time - watch.elapsedMilliseconds()));
                if (binlog_event)
                {
                    got_event = true;
                    ++sum;
                    onEvent(buffers, binlog_event, metadata);
                }

                exception_occur_times = 0;
                /// Do NOT catch exception of `flushBuffersData` here,
                /// or the data will be lost if exception occurs and skip_error_count enabled
            }
            catch (const Exception & e)
            {
                exception_occur_times = std::min(10ul, exception_occur_times + 1);

                if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA && settings->max_wait_time_when_mysql_unavailable >= 0)
                {
                    flushBuffersData(buffers, metadata);
                    recordException();
                    need_reconnect = true;
                    tryLogCurrentException(log, "Lost connect to MySQL and we will retry later");
                    std::this_thread::sleep_for(std::chrono::milliseconds(settings->max_wait_time_when_mysql_unavailable));
                    continue;
                }
                else if (settings->skip_error_count < 0 || settings->skip_error_count > already_skip_errors)
                {
                    /// Print stack of the exception for debug
                    tryLogCurrentException(log, "Skip this error due to MaterializedMySQLSetting [skip_error_count = " + settings->skip_error_count.toString()
                                    + "], already skip " + toString(already_skip_errors) + " errors; this error info: ");
                    if (settings->skip_error_count >= 0)
                        already_skip_errors++;
                }
                else
                    throw;
            }

            /// XXX: Now it's a little heavy that any exception during `flushBuffersData` will trigger rescheduling sync-thread task;
            /// It's better to take different actions for different exceptions
            if (watch.elapsedMilliseconds() > max_flush_time || buffers.checkThresholds(
                    settings->max_rows_in_buffer, settings->max_bytes_in_buffer,
                    settings->max_rows_in_buffers, settings->max_bytes_in_buffers)
                )
            {
                LOG_TRACE(log, "SyncThreadPerf: processed {} events with {} ms", sum - last, watch.elapsedMilliseconds());
                last = sum;

                if (!buffers.data.empty())
                {
                    LOG_DEBUG(log, "Begin to flush buffers: {}", buffers.printBuffersInfo());
                    flushBuffersData(buffers, metadata);
                }
                else
                {
                    /// For some stable tables, the user may seldom update it. Then the binlog may be invalid for these tables.
                    /// For such scenario, we will try to commit position for these inessential events while no data in buffer
                    /// to keep up with the latest binlog position
                    auto current_position = client.getPosition();

                    metadata.transaction(current_position, [&]() { tryCommitPosition(current_position); });
                }
                watch.restart();
            }

            /// Sleep some time to save resources if any exception occurred or no more events were fetched
            if (exception_occur_times > 0 || !got_event)
            {
                auto real_sleep_ms = thread_schedule_interval_ms * (1 << exception_occur_times);
                LOG_TRACE(log, "Reschedule sync thread after {} ms {}", real_sleep_ms, (exception_occur_times > 0 ? " as exception occurred" : ""));
                std::this_thread::sleep_for(std::chrono::milliseconds(real_sleep_ms));
            }
        }
    }
    catch (...)
    {
        recordException();
        /// report sync failed status to manager
        reportSyncFailed();
        tryLogCurrentException(log);
        setSynchronizationThreadException(std::current_exception());

        stopSelf();
    }
}

void MaterializeMySQLSyncThread::stopSynchronization()
{
    if (!sync_quit && background_thread_pool)
    {
        sync_quit = true;
        background_thread_pool->join();
        client.disconnect();

        heartbeat_task->deactivate();
    }
}

void MaterializeMySQLSyncThread::startSynchronization()
{
    background_thread_pool = std::make_unique<ThreadFromGlobalPool>([this]() { synchronization(); });

    last_heartbeat_time = time(nullptr);
    heartbeat_task = getContext()->getSchedulePool().createTask("MaterializedMySQLSyncThread", [this] { heartbeat(); });
    heartbeat_task->activateAndSchedule();
}

void MaterializeMySQLSyncThread::assertMySQLAvailable()
{
    try
    {
        MaterializedMySQL::checkMySQLVariables(pool.get(), getContext()->getSettingsRef());
    }
    catch (const mysqlxx::ConnectionFailed & e)
    {
        if (e.errnum() == MySQLErrorCode::ER_ACCESS_DENIED_ERROR
            || e.errnum() == MySQLErrorCode::ER_DBACCESS_DENIED_ERROR)
            throw Exception("MySQL SYNC USER ACCESS ERR: mysql sync user needs "
                            "at least GLOBAL PRIVILEGES:'RELOAD, REPLICATION SLAVE, REPLICATION CLIENT' "
                            "and SELECT PRIVILEGE on Database " + mysql_database_name
                            , ErrorCodes::SYNC_MYSQL_USER_ACCESS_ERROR);
        else if (e.errnum() == MySQLErrorCode::ER_BAD_DB_ERROR)
            throw Exception("Unknown database '" + mysql_database_name + "' on MySQL", ErrorCodes::UNKNOWN_DATABASE);
        else
            throw;
    }
}

static inline BlockOutputStreamPtr
getTableOutput(const String & database_name, const String & table_name, ContextMutablePtr query_context, bool insert_delete_flag_column = false)
{
    const StoragePtr & storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);

    WriteBufferFromOwnString insert_columns_str;
    auto storage_metadata = storage->getInMemoryMetadataPtr();
    const ColumnsDescription & storage_columns = storage_metadata->getColumns();
    const NamesAndTypesList & insert_columns_names = storage_columns.getOrdinary();


    for (auto iterator = insert_columns_names.begin(); iterator != insert_columns_names.end(); ++iterator)
    {
        if (iterator != insert_columns_names.begin())
            insert_columns_str << ", ";

        insert_columns_str << backQuoteIfNeed(iterator->name);
    }
    if (insert_delete_flag_column)
    {
        insert_columns_str << ", " << backQuoteIfNeed(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME);
    }

    String comment = "Materialize MySQL step 1: execute dump data";
    BlockIO res = tryToExecuteQuery("INSERT INTO " + backQuoteIfNeed(table_name) + "(" + insert_columns_str.str() + ")" + " VALUES",
        query_context, database_name, comment);

    if (!res.out)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    return res.out;
}

static inline UInt32 randomNumber()
{
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max());
    return dist6(rng);
}

bool MaterializeMySQLSyncThread::prepareSynchronized(MaterializeMetadata & metadata)
{
    mysqlxx::PoolWithFailover::Entry connection;

    while (!isCancelled())
    {
        try
        {
            connection = pool.tryGet();
            if (connection.isNull())
            {
                if (settings->max_wait_time_when_mysql_unavailable < 0)
                    throw Exception("Unable to connect to MySQL", ErrorCodes::UNKNOWN_EXCEPTION);
                sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
                continue;
            }

            /// TODO: if this is required
            MaterializedMySQL::checkMySQLVariables(connection, getContext()->getSettingsRef());

            client.connect();
            NameSet dump_tables {assigned_materialized_table};
            client.startBinlogDumpGTID(randomNumber(), mysql_database_name, dump_tables,
                                        metadata.executed_gtid_set, metadata.binlog_file, metadata.binlog_checksum);

            setSynchronizationThreadException(nullptr);
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(log);

            try
            {
                throw;
            }
            catch (const mysqlxx::ConnectionFailed &) {}
            catch (const mysqlxx::BadQuery & e)
            {
                // Lost connection to MySQL server during query
                if (e.code() != MySQLErrorCode::CR_SERVER_LOST || settings->max_wait_time_when_mysql_unavailable < 0)
                    throw;
            }

            setSynchronizationThreadException(std::current_exception());
            /// Avoid busy loop when MySQL is not available.
            sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
        }
    }

    return false;
}

void MaterializeMySQLSyncThread::flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata)
{
    if (buffers.data.empty())
        return;

    auto position = client.getPosition();

    metadata.transaction(position, [&]() { buffers.commit(getContext(), position); });

    LOG_INFO(log, "Finish flushing and MySQL executed position: \n {}", dumpPosition(position));
}

static inline void fillSignColumnsData(Block & data, Int8 sign_value, size_t fill_size)
{
    MutableColumnPtr sign_mutable_column = IColumn::mutate(std::move(data.getByPosition(data.columns() - 1).column));

    ColumnInt8::Container & sign_column_data = assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();

    for (size_t index = 0; index < fill_size; ++index)
        sign_column_data.emplace_back(sign_value);

    data.getByPosition(data.columns() - 1).column = std::move(sign_mutable_column);
}

template <bool assert_nullable = false>
static void writeFieldsToColumn(
    IColumn & column_to, const Row & rows_data, size_t column_index, const std::vector<bool> & mask, ColumnUInt8 * null_map_column = nullptr)
{
    if (ColumnNullable * column_nullable = typeid_cast<ColumnNullable *>(&column_to))
        writeFieldsToColumn<true>(column_nullable->getNestedColumn(), rows_data, column_index, mask, &column_nullable->getNullMapColumn());
    else
    {
        const auto & write_data_to_null_map = [&](const Field & field, size_t row_index)
        {
            if (!mask.empty() && !mask.at(row_index))
                return false;

            if constexpr (assert_nullable)
            {
                if (field.isNull())
                {
                    column_to.insertDefault();
                    null_map_column->insertValue(1);
                    return false;
                }

                null_map_column->insertValue(0);
            }

            return true;
        };

        const auto & write_data_to_column = [&](auto * casted_column, auto from_type, auto to_type)
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                    casted_column->insertValue(static_cast<decltype(to_type)>(value.template get<decltype(from_type)>()));
            }
        };

        if (ColumnInt8 * casted_int8_column = typeid_cast<ColumnInt8 *>(&column_to))
            write_data_to_column(casted_int8_column, UInt64(), Int8());
        else if (ColumnInt16 * casted_int16_column = typeid_cast<ColumnInt16 *>(&column_to))
            write_data_to_column(casted_int16_column, UInt64(), Int16());
        else if (ColumnInt64 * casted_int64_column = typeid_cast<ColumnInt64 *>(&column_to))
            write_data_to_column(casted_int64_column, UInt64(), Int64());
        else if (ColumnUInt8 * casted_uint8_column = typeid_cast<ColumnUInt8 *>(&column_to))
            write_data_to_column(casted_uint8_column, UInt64(), UInt8());
        else if (ColumnUInt16 * casted_uint16_column = typeid_cast<ColumnUInt16 *>(&column_to))
            write_data_to_column(casted_uint16_column, UInt64(), UInt16());
        else if (ColumnUInt32 * casted_uint32_column = typeid_cast<ColumnUInt32 *>(&column_to))
            write_data_to_column(casted_uint32_column, UInt64(), UInt32());
        else if (ColumnUInt64 * casted_uint64_column = typeid_cast<ColumnUInt64 *>(&column_to))
            write_data_to_column(casted_uint64_column, UInt64(), UInt64());
        else if (ColumnFloat32 * casted_float32_column = typeid_cast<ColumnFloat32 *>(&column_to))
            write_data_to_column(casted_float32_column, Float64(), Float32());
        else if (ColumnFloat64 * casted_float64_column = typeid_cast<ColumnFloat64 *>(&column_to))
            write_data_to_column(casted_float64_column, Float64(), Float64());
        else if (ColumnDecimal<Decimal32> * casted_decimal_32_column = typeid_cast<ColumnDecimal<Decimal32> *>(&column_to))
            write_data_to_column(casted_decimal_32_column, Decimal32(), Decimal32());
        else if (ColumnDecimal<Decimal64> * casted_decimal_64_column = typeid_cast<ColumnDecimal<Decimal64> *>(&column_to))
            write_data_to_column(casted_decimal_64_column, Decimal64(), Decimal64());
        else if (ColumnDecimal<Decimal128> * casted_decimal_128_column = typeid_cast<ColumnDecimal<Decimal128> *>(&column_to))
            write_data_to_column(casted_decimal_128_column, Decimal128(), Decimal128());
        else if (ColumnDecimal<Decimal256> * casted_decimal_256_column = typeid_cast<ColumnDecimal<Decimal256> *>(&column_to))
            write_data_to_column(casted_decimal_256_column, Decimal256(), Decimal256());
        else if (ColumnDecimal<DateTime64> * casted_datetime_64_column = typeid_cast<ColumnDecimal<DateTime64> *>(&column_to))
            write_data_to_column(casted_datetime_64_column, DateTime64(), DateTime64());
        else if (ColumnInt32 * casted_int32_column = typeid_cast<ColumnInt32 *>(&column_to))
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                {
                    if (value.getType() == Field::Types::UInt64)
                        casted_int32_column->insertValue(value.get<Int32>());
                    else if (value.getType() == Field::Types::Int64)
                    {
                        /// For MYSQL_TYPE_INT24
                        const Int32 & num = value.get<Int32>();
                        casted_int32_column->insertValue(num & 0x800000 ? num | 0xFF000000 : num);
                    }
                    else
                        throw Exception("LOGICAL ERROR: it is a bug.", ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
        else if (ColumnString * casted_string_column = typeid_cast<ColumnString *>(&column_to))
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                {
                    const String & data = value.get<const String &>();
                    casted_string_column->insertData(data.data(), data.size());
                }
            }
        }
        else if (ColumnFixedString * casted_fixed_string_column = typeid_cast<ColumnFixedString *>(&column_to))
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                {
                    const String & data = value.get<const String &>();
                    casted_fixed_string_column->insertData(data.data(), data.size());
                }
            }
        }
        else
            throw Exception("Unsupported data type from MySQL.", ErrorCodes::NOT_IMPLEMENTED);
    }
}

template <Int8 sign>
static size_t onWriteOrDeleteData(const Row & rows_data, Block & buffer)
{
    size_t prev_bytes = buffer.bytes();
    for (size_t column = 0; column < buffer.columns() - 1; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer.getByPosition(column).column));

        writeFieldsToColumn(*col_to, rows_data, column, {});
        buffer.getByPosition(column).column = std::move(col_to);
    }

    fillSignColumnsData(buffer, sign, rows_data.size());
    return buffer.bytes() - prev_bytes;
}

/// We need to use unique key instead of sorting key as user could use table override func
static inline bool differenceUniqueKeys(const Tuple & row_old_data, const Tuple & row_new_data, const std::vector<size_t> unique_columns_index)
{
    for (const auto & unique_column_index : unique_columns_index)
        if (row_old_data[unique_column_index] != row_new_data[unique_column_index])
            return true;

    return false;
}

static inline size_t onUpdateData(const Row & rows_data, Block & buffer, const std::vector<size_t> & unique_columns_index)
{
    if (rows_data.size() % 2 != 0)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    size_t prev_bytes = buffer.bytes();
    std::vector<bool> writeable_rows_mask(rows_data.size());

    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        writeable_rows_mask[index + 1] = true;
        writeable_rows_mask[index] = differenceUniqueKeys(
            DB::get<const Tuple &>(rows_data[index]), DB::get<const Tuple &>(rows_data[index + 1]), unique_columns_index);
    }

    for (size_t column = 0; column < buffer.columns() - 1; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer.getByPosition(column).column));

        writeFieldsToColumn(*col_to, rows_data, column, writeable_rows_mask);
        buffer.getByPosition(column).column = std::move(col_to);
    }

    MutableColumnPtr sign_mutable_column = IColumn::mutate(std::move(buffer.getByPosition(buffer.columns() - 1).column));

    ColumnInt8::Container & sign_column_data = assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();

    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        if (likely(!writeable_rows_mask[index]))
        {
            sign_column_data.emplace_back(0);
        }
        else
        {
            /// If the sorting keys is modified, we should cancel the old data, but this should not happen frequently
            sign_column_data.emplace_back(1);
            sign_column_data.emplace_back(0);
        }
    }

    buffer.getByPosition(buffer.columns() - 1).column = std::move(sign_mutable_column);
    return buffer.bytes() - prev_bytes;
}

void MaterializeMySQLSyncThread::onEvent(Buffers & buffers, const BinlogEventPtr & receive_event, MaterializeMetadata & metadata)
{
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        Buffers::BufferAndUniqueColumnsPtr buffer = buffers.getTableDataBuffer(write_rows_event.table, getContext());
        size_t bytes = onWriteOrDeleteData<0>(write_rows_event.rows, buffer->first);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), write_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        Buffers::BufferAndUniqueColumnsPtr buffer = buffers.getTableDataBuffer(update_rows_event.table, getContext());
        size_t bytes = onUpdateData(update_rows_event.rows, buffer->first, buffer->second);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), update_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        Buffers::BufferAndUniqueColumnsPtr buffer = buffers.getTableDataBuffer(delete_rows_event.table, getContext());
        size_t bytes = onWriteOrDeleteData<1>(delete_rows_event.rows, buffer->first);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), delete_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        QueryEvent & query_event = static_cast<QueryEvent &>(*receive_event);
        Position position_before_ddl;
        position_before_ddl.update(metadata.binlog_position, metadata.binlog_file, metadata.executed_gtid_set);
        metadata.transaction(position_before_ddl, [&]() { buffers.commit(getContext(), position_before_ddl); });
        metadata.transaction(client.getPosition(),[&](){ executeDDLAtomic(query_event); });
    }
    else
    {
        /// MYSQL_UNHANDLED_EVENT
        if (receive_event->header.type == ROTATE_EVENT)
        {
            /// Some behaviors(such as changing the value of "binlog_checksum") rotate the binlog file.
            /// To ensure that the synchronization continues, we need to handle these events
            metadata.fetchMasterVariablesValue(pool.get());
            client.setBinlogChecksum(metadata.binlog_checksum);
        }
        else if (receive_event->header.type != HEARTBEAT_EVENT)
        {
            const auto & dump_event_message = [&]()
            {
                WriteBufferFromOwnString buf;
                receive_event->dump(buf);
                return buf.str();
            };

            LOG_DEBUG(log, "Skip MySQL event: \n {}", dump_event_message());
        }
    }
}

void MaterializeMySQLSyncThread::commitPosition(const MySQLReplication::Position & position)
{
    if (position.binlog_name.empty())
        return;

    auto database = DatabaseCatalog::instance().getDatabase(database_name, getContext());
    auto materialized_mysql = dynamic_cast<DatabaseCloudMaterializedMySQL *>(database.get());
    if (!materialized_mysql)
        throw Exception("Expect CloudMaterializedMySQL database, but got " + database->getEngineName(), ErrorCodes::LOGICAL_ERROR);

    String binlog_meta_name = getNameForMaterializedBinlog(materialized_mysql->getDatabaseUUID(), assigned_materialized_table);

    auto binlog_metadata = getContext()->getCnchCatalog()->getMaterializedMySQLBinlogMetadata(binlog_meta_name);
    if (!binlog_metadata)
        throw Exception("Cannot get binlog meta from catalog for " + binlog_meta_name, ErrorCodes::LOGICAL_ERROR);

    if (binlog_metadata->binlog_file() != position.binlog_name
         || binlog_metadata->binlog_position() < position.binlog_pos)
    {
        binlog_metadata->set_binlog_file(position.binlog_name);
        binlog_metadata->set_binlog_position(position.binlog_pos);
        binlog_metadata->set_executed_gtid_set(position.gtid_sets.toString());

        LOG_DEBUG(log, "Try to commit position while no data in buffers now: {}", dumpPosition(position));
        getContext()->getCnchCatalog()->setMaterializedMySQLBinlogMetadata(binlog_meta_name, *binlog_metadata);
    }
}

void MaterializeMySQLSyncThread::tryCommitPosition(const MySQLReplication::Position & position)
{
    try
    {
        commitPosition(position);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to commit position for thread #" + thread_key);
    }
}

void MaterializeMySQLSyncThread::executeDDLAtomic(const QueryEvent & query_event)
{
    try
    {
        String query = query_event.query;
        auto ddl_params = parseMySQLDDLQuery(query_event.query);
        if (ddl_params.query_type == MySQLDDLQueryParams::UNKNOWN_TYPE)
        {
            LOG_DEBUG(log, "Skip unsupported MySQL DDL query: {}", query_event.query);
            return;
        }
        else if (!ddl_params.execute_table.empty())
        {
            auto ddl_database_name =  ddl_params.execute_database.empty() ? query_event.schema: ddl_params.execute_database;
            if (ddl_database_name != mysql_database_name
                || (assigned_materialized_table != ddl_params.execute_table && ddl_params.query_type != MySQLDDLQueryParams::CREATE_TABLE))
            {
                LOG_DEBUG(log, "Skip MySQL DDL: {}", query_event.query);
                return;
            }
        }

        auto database = DatabaseCatalog::instance().getDatabase(database_name, getContext());
        auto materialized_mysql = dynamic_cast<DatabaseCloudMaterializedMySQL*>(database.get());
        if (!materialized_mysql)
            throw Exception("Database should be CloudMaterializedMySQL", ErrorCodes::LOGICAL_ERROR);
        auto server_host_port = materialized_mysql->getServerClientOfManager();
        auto server_client = getContext()->getCnchServerClient(server_host_port.getHost(), server_host_port.rpc_port);
        if (!server_client)
            throw Exception("Failed to get server client for " + server_host_port.toDebugString(), ErrorCodes::LOGICAL_ERROR);

        auto pos = database_name.find("_" + thread_key);
        if (pos == std::string::npos)
            throw Exception(
                "Local MaterializedMySQL database #" + database_name + " has no thread key suffix #" + thread_key,
                 ErrorCodes::LOGICAL_ERROR
            );
        String cnch_database_name = database_name.substr(0, pos);

        auto position = client.getPosition();
        MySQLBinLogInfo binlog;
        binlog.binlog_file = position.binlog_name;
        binlog.binlog_position = position.binlog_pos;
        binlog.executed_gtid_set = position.gtid_sets.toString();

        server_client->submitMaterializedMySQLDDLQuery(cnch_database_name, thread_key, query, binlog);
    }
    catch (Exception & exception)
    {
        exception.addMessage("While executing MYSQL_QUERY_EVENT. The query: " + query_event.query);

        tryLogCurrentException(log);

        /// If some DDL query was not successfully parsed and executed
        /// Then replication may fail on next binlog events anyway
        if (exception.code() != ErrorCodes::SYNTAX_ERROR)
            throw;
    }
}

bool MaterializeMySQLSyncThread::isMySQLSyncThread()
{
    return getThreadName() == MYSQL_BACKGROUND_THREAD_NAME;
}

void MaterializeMySQLSyncThread::setSynchronizationThreadException(const std::exception_ptr & exception)
{
    if (exception)
        LOG_ERROR(log, "Exception occurs in Synchronization Thread: {}", getExceptionMessage(exception, false));
}

void MaterializeMySQLSyncThread::recordException(bool create_log, const String & resync_table)
{
    MaterializedMySQL::recordException(MaterializedMySQLLogElement::WORKER_THREAD, getContext(), database_name, {assigned_materialized_table}, create_log, resync_table);
}

void MaterializeMySQLSyncThread::Buffers::add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes)
{
    total_blocks_rows += written_rows;
    total_blocks_bytes += written_bytes;
    max_block_rows = std::max(block_rows, max_block_rows);
    max_block_bytes = std::max(block_bytes, max_block_bytes);
}

bool MaterializeMySQLSyncThread::Buffers::checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const
{
    return max_block_rows >= check_block_rows || max_block_bytes >= check_block_bytes || total_blocks_rows >= check_total_rows
        || total_blocks_bytes >= check_total_bytes;
}

void MaterializeMySQLSyncThread::Buffers::prepareCommit(ContextMutablePtr query_context, const String & table_name, const MySQLBinLogInfo & binlog)
{
    auto table = DatabaseCatalog::instance().getTable(StorageID(database, table_name), query_context);
    auto host_with_port = query_context->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), false);

    auto host_address = host_with_port.getHost();
    auto host_port = host_with_port.rpc_port;
    auto server_client = query_context->getCnchServerClient(host_address, host_port);
    if (!server_client)
        throw Exception("Failed to get ServerClient", ErrorCodes::LOGICAL_ERROR);

    LOG_TRACE(getLogger("MaterializedMySQLSyncThread"), "Try to commit buffer data to server: {}", server_client->getRPCAddress());
    /// set rpc info for committing later
    auto & client_info = query_context->getClientInfo();

    client_info.current_address = Poco::Net::SocketAddress(createHostPortString(host_address, host_port));
    client_info.rpc_port = host_port;

    /// Create transaction for committing data
    auto txn = std::make_shared<CnchWorkerTransaction>(query_context, server_client);
    txn->setBinlogInfo(binlog);
    query_context->setCurrentTransaction(txn);
}

void MaterializeMySQLSyncThread::Buffers::commit(ContextPtr context, const MySQLReplication::Position & position)
{
    try
    {
        if (position.binlog_name.empty())
            throw Exception("binlog is empty when try to committing data for MaterializedMySQL", ErrorCodes::LOGICAL_ERROR);

        MySQLBinLogInfo binlog;
        binlog.binlog_file = position.binlog_name;
        binlog.binlog_position = position.binlog_pos;
        binlog.executed_gtid_set = position.gtid_sets.toString();

        /// XXX: Here we can just support for one table to flush
        for (auto & table_name_and_buffer : data)
        {
            auto query_context = MaterializedMySQL::createQueryContext(context);

            prepareCommit(query_context, table_name_and_buffer.first + "_" + table_suffix, binlog);

            OneBlockInputStream input(table_name_and_buffer.second->first);
            BlockOutputStreamPtr out = getTableOutput(database, table_name_and_buffer.first + "_" + table_suffix, query_context, true);
            Stopwatch watch;
            copyData(input, *out);
            LOG_DEBUG(getLogger("MaterializeMySQLThread ({})"), "Copied {} rows and elapsed {} ms",
                table_name_and_buffer.first, table_name_and_buffer.second->first.rows(), watch.elapsedMilliseconds());
        }

        data.clear();
        max_block_rows = 0;
        max_block_bytes = 0;
        total_blocks_rows = 0;
        total_blocks_bytes = 0;
    }
    catch (...)
    {
        data.clear();
        throw;
    }
}

MaterializeMySQLSyncThread::Buffers::BufferAndUniqueColumnsPtr MaterializeMySQLSyncThread::Buffers::getTableDataBuffer(
    const String & table_name, ContextPtr context)
{
    const auto & iterator = data.find(table_name);
    if (iterator == data.end())
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(database, table_name + "_" + table_suffix), context);

        auto metadata = storage->getInMemoryMetadataPtr();
        BufferAndUniqueColumnsPtr & buffer_and_unique_columns = data.try_emplace(
            table_name, std::make_shared<BufferAndUniqueColumns>(metadata->getSampleBlockWithDeleteFlag(), std::vector<size_t>{})).first->second;

        /// We need to use unique key instead of sorting key as user could use table override func
        Names required_for_unique_key = metadata->getColumnsRequiredForUniqueKey();

        for (const auto & required_name_for_unique_key : required_for_unique_key)
            buffer_and_unique_columns->second.emplace_back(
                buffer_and_unique_columns->first.getPositionByName(required_name_for_unique_key));

        return buffer_and_unique_columns;
    }

    return iterator->second;
}

String MaterializeMySQLSyncThread::Buffers::printBuffersInfo()
{
    return "Total rows [" + toString(total_blocks_rows) + "]; total Bytes [" + toString(total_blocks_bytes) + "]";
}

void MaterializeMySQLSyncThread::heartbeat()
{
    try
    {
        auto database = DatabaseCatalog::instance().getDatabase(database_name, getContext());
        auto materialized_mysql = dynamic_cast<DatabaseCloudMaterializedMySQL*>(database.get());
        if (!materialized_mysql)
            throw Exception("Database should be CloudMaterializedMySQL, but got " + database->getEngineName(), ErrorCodes::LOGICAL_ERROR);

        auto server_host_port = materialized_mysql->getServerClientOfManager();
        auto server_client = getContext()->getCnchServerClient(server_host_port.getHost(), server_host_port.rpc_port);
        if (!server_client)
            throw Exception("Failed to get server client with info: " + server_host_port.toDebugString(), ErrorCodes::LOGICAL_ERROR);

        auto pos = database_name.find("_" + thread_key);
        if (pos == std::string::npos)
            throw Exception("Local MaterializedMySQL database #" + database_name + " has no thread key suffix #" + thread_key, ErrorCodes::LOGICAL_ERROR);

        String cnch_database_name = database_name.substr(0, pos);
        server_client->reportHeartBeatForSyncThread(cnch_database_name, thread_key);
        LOG_TRACE(log, "Successfully to report heart beat for " + cnch_database_name + "#" + thread_key);

        last_heartbeat_time = time(nullptr);
    }
    catch (...)
    {
        auto now = time(nullptr);
        if (UInt64(now - last_heartbeat_time) > settings->sync_thread_max_heartbeat_interval_second)
        {
            LOG_ERROR(log, "Sync thread will shutdown as heartbeat reached time out.");
            stopSelf();
            return;
        }
    }

    heartbeat_task->scheduleAfter(settings->sync_thread_heartbeat_interval_ms);
}

void MaterializeMySQLSyncThread::reportSyncFailed()
{
    try
    {
        auto database = DatabaseCatalog::instance().getDatabase(database_name, getContext());
        auto materialized_mysql = dynamic_cast<DatabaseCloudMaterializedMySQL*>(database.get());
        if (!materialized_mysql)
            throw Exception("Database should be CloudMaterializedMySQL, but got " + database->getEngineName(), ErrorCodes::LOGICAL_ERROR);

        auto server_host_port = materialized_mysql->getServerClientOfManager();
        auto server_client = getContext()->getCnchServerClient(server_host_port.getHost(), server_host_port.rpc_port);
        if (!server_client)
            throw Exception("Failed to get server client with info: " + server_host_port.toDebugString(), ErrorCodes::LOGICAL_ERROR);

        auto pos = database_name.find("_" + thread_key);
        if (pos == std::string::npos)
            throw Exception("Local MaterializedMySQL database #" + database_name + " has no thread key suffix #" + thread_key, ErrorCodes::LOGICAL_ERROR);

        String cnch_database_name = database_name.substr(0, pos);
        server_client->reportSyncFailedForSyncThread(cnch_database_name, thread_key);
        LOG_TRACE(log, "Successfully to report sync failed status for " + cnch_database_name + "#" + thread_key);
    }
    catch (...)
    {
        stopSelf();
    }
}

void MaterializeMySQLSyncThread::stopSelf()
{
    ThreadFromGlobalPool([c = Context::createCopy(getContext()), thread_id = thread_key, db_name = database_name] {
        try
        {
            MySQLSyncThreadCommand command;
            command.type = MySQLSyncThreadCommand::STOP_SYNC;
            command.sync_thread_key = thread_id;
            command.database_name = db_name;

            /// call executeSyncThreadTaskCommand to trigger DROP table and database
            executeSyncThreadTaskCommand(command, c);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__ );
        }
    }).detach();
}

}

#endif
