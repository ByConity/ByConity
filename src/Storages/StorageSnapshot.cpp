#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/StorageView.h>
#include <sparsehash/dense_hash_set>
#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
}

void ObjectSchemas::reset(const ObjectAssembledSchema & assembled_schema, const ObjectPartialSchemas & partial_schemas)
{
    std::lock_guard<std::mutex> lock(partial_schema_refresh_mutex);
    partial_object_schemas = partial_schemas;

    partial_object_schemas.emplace(OBJECT_GLOBAL_SCHEMA_TXN, assembled_schema);
}

bool ObjectSchemas::isEmpty() const
{
    std::lock_guard<std::mutex> lock(partial_schema_refresh_mutex);
    return partial_object_schemas.empty();
}

void ObjectSchemas::appendPartialSchema(const TxnTimestamp & txn_id, ObjectPartialSchema partial_schema)
{
    std::lock_guard<std::mutex> lock(partial_schema_refresh_mutex);
    partial_object_schemas.emplace(txn_id, partial_schema);
}

void ObjectSchemas::refreshAssembledSchema(const ObjectAssembledSchema & assembled_schema, std::vector<TxnTimestamp> txn_ids)
{
    std::lock_guard<std::mutex> lock(partial_schema_refresh_mutex);
    
    partial_object_schemas[OBJECT_GLOBAL_SCHEMA_TXN] = assembled_schema;
    for (const auto & txn_id : txn_ids)
    {
        partial_object_schemas.erase(txn_id);
    }
}

ObjectAssembledSchema ObjectSchemas::assembleSchema(const ContextPtr query_context, const StorageMetadataPtr & metadata) const
{
    std::lock_guard<std::mutex> lock(partial_schema_refresh_mutex);
    std::vector<TxnTimestamp> unfiltered_txn_ids;
    std::for_each(
        partial_object_schemas.begin(), partial_object_schemas.end(), [&unfiltered_txn_ids](const auto & unfilter_partial_schema) {
            unfiltered_txn_ids.emplace_back(unfilter_partial_schema.first);
        });

    auto committed_partial_schema_txnids = query_context->getCnchCatalog()->filterUncommittedObjectPartialSchemas(unfiltered_txn_ids);

    ObjectPartialSchemas committed_partial_schemas;
    std::for_each(
        committed_partial_schema_txnids.begin(),
        committed_partial_schema_txnids.end(),
        [this, &committed_partial_schemas](const auto & txn_id) {
            auto it = partial_object_schemas.find(txn_id);
            if (it != partial_object_schemas.end())
                committed_partial_schemas[txn_id] = it->second;
        });

    return DB::getConcreteObjectColumns(
        committed_partial_schemas.begin(), committed_partial_schemas.end(), metadata->columns, [](const auto & partial_schema) {
            return partial_schema.second;
        });
}

void ObjectSchemas::dropAbortedPartialSchema(const TxnTimestamp &txn_id)
{
    std::lock_guard<std::mutex> lock(partial_schema_refresh_mutex);
    partial_object_schemas.erase(txn_id);
}

void StorageSnapshot::init()
{
    for (const auto & [name, type] : storage.getVirtuals())
        virtual_columns[name] = type;

    // if (storage.hasLightweightDeletedMask())
    //     system_columns[LightweightDeleteDescription::FILTER_COLUMN.name] = LightweightDeleteDescription::FILTER_COLUMN.type;
}

NamesAndTypesList StorageSnapshot::getColumns(const GetColumnsOptions & options) const
{
    auto all_columns = getMetadataForQuery()->getColumns().get(options);

    if (options.with_extended_objects)
        extendObjectColumns(all_columns, object_columns, options.with_subcolumns);

    NameSet column_names;
    if (options.with_virtuals)
    {
        /// Virtual columns must be appended after ordinary,
        /// because user can override them.
        if (!virtual_columns.empty())
        {
            for (const auto & column : all_columns)
                column_names.insert(column.name);

            for (const auto & [name, type] : virtual_columns)
                if (!column_names.contains(name))
                    all_columns.emplace_back(name, type);
        }
    }

    if (options.with_system_columns)
    {
        if (!system_columns.empty() && column_names.empty())
        {
            for (const auto & column : all_columns)
                column_names.insert(column.name);
        }

        for (const auto & [name, type] : system_columns)
            if (!column_names.contains(name))
                all_columns.emplace_back(name, type);
    }

    return all_columns;
}

NamesAndTypesList StorageSnapshot::getColumnsByNames(const GetColumnsOptions & options, const Names & names) const
{
    NamesAndTypesList res;
    for (const auto & name : names)
        res.push_back(getColumn(options, name));
    return res;
}

std::optional<NameAndTypePair> StorageSnapshot::tryGetColumn(const GetColumnsOptions & options, const String & column_name) const
{
    const auto & columns = getMetadataForQuery()->getColumns();
    auto column = columns.tryGetColumn(options, column_name);
    if (column && (!column->type->hasDynamicSubcolumns() || !options.with_extended_objects))
        return column;

    if (options.with_extended_objects)
    {
        auto object_column = object_columns.tryGetColumn(options, column_name);
        if (object_column)
            return object_column;
    }

    if (options.with_virtuals)
    {
        auto it = virtual_columns.find(column_name);
        if (it != virtual_columns.end())
            return NameAndTypePair(column_name, it->second);
    }

    if (options.with_system_columns)
    {
        auto it = system_columns.find(column_name);
        if (it != system_columns.end())
            return NameAndTypePair(column_name, it->second);
    }

    return {};
}

NameAndTypePair StorageSnapshot::getColumn(const GetColumnsOptions & options, const String & column_name) const
{
    auto column = tryGetColumn(options, column_name);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table", column_name);

    return *column;
}

Block StorageSnapshot::getSampleBlockForColumns(
    const Names & column_names, const NameToNameMap & parameter_values, BitEngineReadType bitengine_read_type) const
{
    Block res;

    const auto & columns = getMetadataForQuery()->getColumns();
    for (const auto & column_name : column_names)
    {
        std::string substituted_column_name = column_name;

        /// substituted_column_name is used for parameterized view (which are created using query parameters
        /// and SELECT is used with substitution of these query parameters )
        if (!parameter_values.empty())
            substituted_column_name = StorageView::replaceValueWithQueryParameter(column_name, parameter_values);

        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, substituted_column_name);
        auto object_column = object_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, substituted_column_name);
        if (column && !object_column)
        {
            if (isBitmap64(column->type) && column->type->isBitEngineEncode() && bitengine_read_type == BitEngineReadType::ONLY_ENCODE)
                column->name += BITENGINE_COLUMN_EXTENSION;
            res.insert({column->type->createColumn(), column->type, column->name});
        }
        else if (object_column)
        {
            res.insert({object_column->type->createColumn(), object_column->type, column_name});
        }
        else if (auto it = virtual_columns.find(column_name); it != virtual_columns.end())
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            const auto & type = it->second;
            res.insert({type->createColumn(), type, column_name});
        }
        else
        {
            throw Exception(
                "Column " + backQuote(column_name) + " not found in table "
                    + (storage.getStorageID().empty() ? "" : storage.getStorageID().getNameForLogs()),
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }
    return res;
}

ColumnsDescription StorageSnapshot::getDescriptionForColumns(const Names & column_names) const
{
    ColumnsDescription res;
    const auto & columns = getMetadataForQuery()->getColumns();
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumnDescription(GetColumnsOptions::All, name);
        auto object_column = object_columns.tryGetColumnOrSubcolumnDescription(GetColumnsOptions::All, name);
        if (column && !object_column)
        {
            res.add(*column, "", false, false);
        }
        else if (object_column)
        {
            res.add(*object_column, "", false, false);
        }
        else if (auto it = virtual_columns.find(name); it != virtual_columns.end())
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            const auto & type = it->second;
            res.add({name, type});
        }
        else
        {
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                            "Column {} not found in table {}", backQuote(name), storage.getStorageID().getNameForLogs());
        }
    }

    return res;
}

namespace
{
    using DenseHashSet = google::dense_hash_set<StringRef, StringRefHash>;
}

void StorageSnapshot::check(const Names & column_names) const
{
    const auto & columns = getMetadataForQuery()->getColumns();
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();

    if (column_names.empty())
    {
        auto list_of_columns = listOfColumns(columns.get(options));
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}", list_of_columns);
    }

    DenseHashSet unique_names;
    unique_names.set_empty_key(StringRef());

    auto func_columns = metadata->getFuncColumns();

    for (const auto & name : column_names)
    {
        if (isMapImplicitKey(name)) continue;

        // ignore checking functional columns
        if (func_columns.contains(name))
            continue;
        
        bool has_column = columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || object_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || virtual_columns.contains(name);

        if (!has_column)
        {
            auto list_of_columns = listOfColumns(columns.get(options));
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: {}",
                backQuote(name), storage.getStorageID().getNameForLogs(), list_of_columns);
        }

        if (unique_names.count(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

DataTypePtr StorageSnapshot::getConcreteType(const String & column_name) const
{
    auto object_column = object_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
    if (object_column)
        return object_column->type;

    return metadata->getColumns().get(column_name).type;
}

NamesAndTypesList StorageSnapshot::getSubcolumnsOfObjectColumns() const
{
    auto all_columns = getMetadataForQuery()->getColumns().get(GetColumnsOptions::All);
    auto size = all_columns.size();
    extendObjectColumns(all_columns, object_columns, true);
    auto start_it = all_columns.begin();
    std::advance(start_it, size);
    NamesAndTypesList result;
    result.splice(result.end(), all_columns, start_it, all_columns.end());
    return result;
}
}
