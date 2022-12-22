#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/IStorageCnchKafka.h>
#include <Storages/AlterCommands.h>

#include <common/shared_ptr_helper.h>

namespace DB
{
String & removeKafkaPrefix(String & name);
String & addKafkaPrefix(String & name);

/***
 * @name StorageCnchKafka
 * @desc Class for Cnch Kafka table in server side, which is the table definition of Kafka table
 *       and used to create KafkaConsumeManager only. It won't execute consumption task.
 * **/
class StorageCnchKafka : public shared_ptr_helper<StorageCnchKafka>, public IStorageCnchKafka
{
    friend struct shared_ptr_helper<StorageCnchKafka>;
public:
    ~StorageCnchKafka() override = default;
    String getName() const override {return "CnchKafka";}

    void startup() override;
    void shutdown() override;
    void drop() override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const override;
    void alter(const AlterCommands & commands, ContextPtr context, TableLockHolder & alter_lock_holder) override;

    ContextPtr getGlobalContext() const { return getContext()->getGlobalContext(); }

    bool tableIsActive() const;
    StoragePtr tryGetTargetTable();

private:
    Poco::Logger * log;

protected:
    StorageCnchKafka(
        const StorageID & table_id_,
        ContextMutablePtr context_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const ASTPtr setting_changes_,
        const KafkaSettings & settings
    );
};

} // namespace DB
#endif

