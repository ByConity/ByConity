#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/StorageReplicaComponent.h>
#include <Storages/Kafka/HaKafkaRestartingThread.h>
#include <Storages/Kafka/HaKafkaAddress.h>
#include <Storages/Kafka/HaReadBufferFromKafkaConsumer.h>
#include <Storages/AlterCommands.h>
#include <Storages/Thresholds.h>
#include <Storages/Kafka/KafkaConsumeInfo.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Processors/Pipe.h>
#include <Interpreters/KafkaLog.h>
#include <Poco/Event.h>
#include <common/shared_ptr_helper.h>

#include <cppkafka/cppkafka.h>
#include <mutex>

namespace DB
{

using DatabaseAndTableName = std::pair<String, String>;


/** Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
  * or as a basic building block for creating pipelines with a continuous insertion / ETL.
  */
class StorageHaKafka : public shared_ptr_helper<StorageHaKafka>, protected StorageReplicaComponent, public IStorage
{
friend struct shared_ptr_helper<StorageHaKafka>;
friend class HaKafkaBlockInputStream;
friend class HaKafkaRestartingThread;

public:
    ~StorageHaKafka() override;

    struct Status : public StorageReplicaComponent::Status
    {
        enum Switch {
            OFF = 0,
            ON = 1,
        };

        String kafka_cluster;
        Strings topics;
        String group_name;
        Strings assigned_partitions;
        Switch consumer_switch;
        UInt32 dependencies;
        UInt32 num_consumers;
        UInt8 is_consuming;
        String last_exception;
    };

    void getStatus(Status & res);

    String getName() const override { return "HaKafka"; }

    void startup() override;
    void shutdown(/*bool should_close_metastore = true*/) override;

    void removeZKNodes();
    void drop() override;
    void alter(const AlterCommands & params, ContextPtr context, TableLockHolder & table_lock_holder) override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;


    void rename(const String & /*new_path_to_db*/, const StorageID & new_table_id) override;

    void updateDependencies(); /// TODO: Not override now

    const auto & getTopics() const { return params.topics; }
    const auto & getFormatName() const { return settings.format.value; }
    const auto & getSchemaName() const { return settings.schema.value; }
    void startConsume();
    void stopConsume();
    void restartConsume();

    NamesAndTypesList getVirtuals() const override;
    Names getVirtualColumnNames() const;
private:

    // Configuration and state
    String full_path;
    String database_name;
    String table_name;

    KafkaSettings settings;
    const SettingsChanges settings_adjustments;

    struct KafkaParams
    {
        Names topics;
        Int64 shard_count = 1;
        Int64 partition_num = RD_KAFKA_PARTITION_UA;
        String leader_priority_template;
        String leader_priority;
        Thresholds memory_table_min_thresholds;
        Thresholds memory_table_max_thresholds;
    };
    KafkaParams params;

    time_t absolute_delay_of_dependencies = 0;
    zkutil::EphemeralNodeHolderPtr absolute_delay_node;
    HaKafkaRestartingThread restarting_thread;
    Poco::Logger * log;

    std::atomic_bool stop_consume {false};
    struct ConsumerContext
    {
        std::unique_ptr<std::timed_mutex> mutex;
        size_t index;
        BackgroundSchedulePool::TaskHolder task;
        BufferPtr buffer;

        /// for error event
        bool error_event;
    };

    std::mutex context_mutex; /// lock for some async callbacks
    std::vector<ConsumerContext> consumer_contexts;

    void initConsumerContexts();
    void resetConsumerContext(size_t i);

    std::mutex status_mutex;
    std::atomic_bool is_consuming{false};
    String last_exception;
    std::vector<cppkafka::TopicPartitionList> assigned_partitions;

    /// KafkaOffsetManagerPtr offset_manager;

    /// buffer table as memory table
    /// MemoryTables memory_tables;
    /// std::mutex memory_table_mtx;

    time_t last_lookup_time = 0;

    UInt64 failed_time {0};

    void checkAndLoadSettings(KafkaSettings & settings, KafkaParams & params);

    cppkafka::Configuration createConsumerConfiguration(size_t consumer_index);
    size_t calcConsumerDistance(KafkaConsumer & consumer);
    BufferPtr createBuffer(size_t consumer_index);
    BufferPtr tryClaimBuffer(size_t consumer_index, long wait_ms);
    void createBuffers();
    void pushBuffer(size_t consumer_index);
    void subscribeBuffer(BufferPtr & buffer, size_t consumer_index);
    void unsubscribeBuffer(BufferPtr & buffer);

    void streamThread(size_t consumer_index);
    bool streamToViews(const Names & required_column_name, size_t consumer_index);
    void streamCopyData(IBlockInputStream & from, IBlockOutputStream & to, size_t consumer_index);
    bool checkDependencies(const StorageID & table_id);
    bool hasUniqueTableDependencies();

    Names collectRequiredColumnsFromDependencies(const Dependencies & dependencies);
    Names collectRequiredColumnsFromTable(const StorageID & table_id);
    Names filterVirtualNames(const Names & names) const;
    ASTPtr constructInsertQuery(const Names & required_virtual_names);

    void createStopMarkFile();
    void removeStopMarkFile();
    bool checkStopMarkFile();

    void createTableIfNotExists();
    void createReplica();
    void attachReplica(); /// backward compatible
    void checkTableStructure(bool skip_sanity_checks, bool allow_alter);

    void assertNotReadonly() const;

    void startStreamThreads();
    void stopStreamThreads();

    void enterLeaderElection() override;
    void exitLeaderElection() override;

    void updateLeaderPriority();
    void updateAbsoluteDelayOfDependencies();
    bool checkYieldLeaderShip();

    HaKafkaAddress getHaKafkaAddress();
    HaKafkaAddress getReplicaAddress(const String & replica_name);

    KafkaLogElement createKafkaLog(KafkaLogElement::Type type, size_t consumer_index);

    void changeSettings(const AlterCommands & params);
    SettingsChanges createSettingsAdjustments();

protected:
    StorageHaKafka(
            const String & zookeeper_path_,
            const String & replica_name_,
            bool attach_,
            const String & path_,
            const StorageID & table_id_,
            ContextPtr context_,
            const ColumnsDescription & columns_,
            const ConstraintsDescription & constraints_,
            const KafkaSettings & settings_);

public:
    bool enableMemoryTable() const { return settings.enable_memory_table;}
    bool isLeader() const {return is_leader;}
};

}
