#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <Protos/data_models.pb.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Transaction/TxnTimestamp.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IStorage;

struct TxnTimestampHasher
{
    size_t operator()(const TxnTimestamp & txn) const
    {
        return std::hash<UInt64>()(txn.toUInt64());
    }
};

// Dynamic object column schema related
const static TxnTimestamp OBJECT_GLOBAL_SCHEMA_TXN = TxnTimestamp{0};
using ObjectPartialSchemaStatus = Protos::TransactionStatus;
using ObjectPartialSchemaStatuses = std::unordered_map<TxnTimestamp, ObjectPartialSchemaStatus, TxnTimestampHasher>;
using ObjectPartialSchemas = std::unordered_map<TxnTimestamp, ColumnsDescription, TxnTimestampHasher>;
using ObjectAssembledSchema = ColumnsDescription;
using ObjectPartialSchema = ColumnsDescription;

struct ObjectSchemas
{
    static String serializeObjectPartialSchemaStatus(const ObjectPartialSchemaStatus & status)
    {
        return Protos::TransactionStatus_Name(status);
    }

    static ObjectPartialSchemaStatus deserializeObjectPartialSchemaStatus(const String & status_str)
    {
        ObjectPartialSchemaStatus status;
        Protos::TransactionStatus_Parse(status_str, &status);
        return status;
    }

    mutable std::mutex partial_schema_refresh_mutex;
    // Including assembled schema, use default key OBJECT_GLOBAL_SCHEMA_TXN
    ObjectPartialSchemas partial_object_schemas;

    void reset(const ObjectAssembledSchema & assembled_schema, const ObjectPartialSchemas & partial_schemas);

    bool isEmpty() const;

    void appendPartialSchema(const TxnTimestamp & txn_id, ObjectPartialSchema partial_schema);

    void refreshAssembledSchema(const ObjectAssembledSchema & assembled_schema,  std::vector<TxnTimestamp> txn_ids);

    ObjectAssembledSchema assembleSchema(ContextPtr query_context, const StorageMetadataPtr & metadata) const;

    void dropAbortedPartialSchema(const TxnTimestamp & txn_id);
};

/// Snapshot of storage that fixes set columns that can be read in query.
/// There are 3 sources of columns: regular columns from metadata,
/// dynamic columns from object Types, virtual columns.
struct StorageSnapshot
{
    const IStorage & storage;
    const StorageMetadataPtr metadata;
    const ColumnsDescription object_columns;

    /// Additional data, on which set of columns may depend.
    /// E.g. data parts in MergeTree, list of blocks in Memory, etc.
    struct Data
    {
        virtual ~Data() = default;
    };

    using DataPtr = std::unique_ptr<Data>;
    DataPtr data;

    /// Projection that is used in query.
    mutable const ProjectionDescription * projection = nullptr;

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_)
        : storage(storage_), metadata(metadata_)
    {
        init();
    }

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_,
        const ColumnsDescription & object_columns_)
        : storage(storage_), metadata(metadata_), object_columns(object_columns_)
    {
        init();
    }

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_,
        const ColumnsDescription & object_columns_,
        DataPtr data_)
        : storage(storage_), metadata(metadata_), object_columns(object_columns_), data(std::move(data_))
    {
        init();
    }

    /// Get all available columns with types according to options.
    NamesAndTypesList getColumns(const GetColumnsOptions & options) const;

    /// Get columns with types according to options only for requested names.
    NamesAndTypesList getColumnsByNames(const GetColumnsOptions & options, const Names & names) const;

    /// Get column with type according to options for requested name.
    std::optional<NameAndTypePair> tryGetColumn(const GetColumnsOptions & options, const String & column_name) const;
    NameAndTypePair getColumn(const GetColumnsOptions & options, const String & column_name) const;

    /// Block with ordinary + materialized + aliases + virtuals + subcolumns.
    Block getSampleBlockForColumns(
        const Names & column_names,
        const NameToNameMap & parameter_values = {},
        BitEngineReadType bitengine_read_type = BitEngineReadType::ONLY_SOURCE) const;

    ColumnsDescription getDescriptionForColumns(const Names & column_names) const;

    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names) const;

    DataTypePtr getConcreteType(const String & column_name) const;

    void addProjection(const ProjectionDescription * projection_) const { projection = projection_; }

    /// If we have a projection then we should use its metadata.
    StorageMetadataPtr getMetadataForQuery() const { return projection ? projection->metadata : metadata; }

    NamesAndTypesList getSubcolumnsOfObjectColumns() const;

private:
    void init();

    std::unordered_map<String, DataTypePtr> virtual_columns;

    /// System columns are not visible in the schema but might be persisted in the data.
    /// One example of such column is lightweight delete mask '_row_exists'.
    std::unordered_map<String, DataTypePtr> system_columns;
};

using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

}
