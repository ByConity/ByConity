#include <memory>
#include <optional>
#include <Storages/StorageDictCloudMergeTree.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageFactory.h>
#include <Core/Block.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CNCH_TRANSACTION_COMMIT_TIMEOUT;
    extern const int METASTORE_OPERATION_ERROR;
}

StorageDictCloudMergeTree::StorageDictCloudMergeTree(
    const StorageID & table_id_,
    String cnch_database_name_,
    String cnch_table_name_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeMetaBase::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : StorageCloudMergeTree(
        table_id_,
        std::move(cnch_database_name_),
        std::move(cnch_table_name_),
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_)
    )
    , split_writer(*this, IStorage::StorageLocation::AUXILITY)
{
    log = ::getLogger(table_id_.getNameForLogs() + " (DictCloudMergeTree)");

    init();
}

void StorageDictCloudMergeTree::init()
{
    NamesAndTypesList physical_columns = getInMemoryMetadataPtr()->getColumns().getAllPhysical();

    LOG_DEBUG(log, "Init stoarge with " + physical_columns.toString());

    /**
     * it is assumed that only three column we have,
     * column : key, value, split
     */
    if (physical_columns.size() != 3)
        throw Exception("Only three column accepted in DictCouldMergeTree", ErrorCodes::BAD_ARGUMENTS);

    dict_column_types = physical_columns;

    auto key_type = physical_columns.begin()->type;
}

template <typename KEY_TYPE>
MutableColumnPtr StorageDictCloudMergeTree::generateKeyConstraintColumn(const ColumnBitMap64 & bitmap_column)
{
    MutableColumnPtr constraint_key_column = ColumnVector<KEY_TYPE>::create();
    typename ColumnVector<KEY_TYPE>::Container & column_data
        = typeid_cast<ColumnVector<KEY_TYPE> *>(constraint_key_column.get())->getData();
    column_data.reserve(bitmap_column.size());

    for (size_t i = 0; i < bitmap_column.size(); ++i)
    {
        const auto & bitmap = bitmap_column.getBitMapAt(i);
        column_data.emplace_back(*bitmap.begin());
    }

    return constraint_key_column;
}

void registerStorageDictCloud(StorageFactory & factory)
{
    auto create = [](const StorageFactory::Arguments & args) -> StoragePtr{
        bool is_extended_storage_def = args.storage_def->partition_by || args.storage_def->primary_key || args.storage_def->order_by
        || args.storage_def->unique_key || args.storage_def->settings || args.storage_def->cluster_by;

        MergeTreeMetaBase::MergingParams merging_params;
        merging_params.mode = MergeTreeMetaBase::MergingParams::Ordinary;

        size_t min_num_params = 0;
        size_t max_num_params = 0;
        String needed_params;

        auto add_mandatory_param = [&](const char * desc)
        {
            ++min_num_params;
            ++max_num_params;
            needed_params += needed_params.empty() ? "\n" : ",\n";
            needed_params += desc;
        };
        auto add_optional_param = [&](const char * desc)
        {
            ++max_num_params;
            needed_params += needed_params.empty() ? "\n" : ",\n[";
            needed_params += desc;
            needed_params += "]";
        };

        add_mandatory_param("database name of Dict");
        add_mandatory_param("table name of Dict");

        if (!is_extended_storage_def)
        {
            add_mandatory_param("name of column with date");
            add_optional_param("sampling element of primary key");
            add_mandatory_param("primary key expression");
            add_mandatory_param("index granularity");
        }

        ASTs & engine_args = args.engine_args;
        size_t arg_num = 0;
        size_t arg_cnt = engine_args.size();

        if (arg_cnt < min_num_params || arg_cnt > max_num_params)
        {
            String msg;
            if (is_extended_storage_def)
                msg += "With extended storage definition syntax storage " + args.engine_name + " requires ";
            else
                msg += "Storage " + args.engine_name + " requires ";

            if (max_num_params)
            {
                if (min_num_params == max_num_params)
                    msg += toString(min_num_params) + " parameters: ";
                else
                    msg += toString(min_num_params) + " to " + toString(max_num_params) + " parameters: ";
                msg += needed_params;
            }
            else
                msg += "no parameters";

            // msg += getMergeTreeVerboseHelp(is_extended_storage_def);

            throw Exception(msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        String cnch_database_name;
        String cnch_table_name;

        if (arg_num + 2 > arg_cnt || !engine_args[arg_num]->as<ASTIdentifier>() || !engine_args[arg_num + 1]->as<ASTIdentifier>())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Expected two string literal arguments for DictCloudMergeTree: cnch_database_name and cnch_table_name");

        cnch_database_name = engine_args[arg_num]->as<ASTIdentifier &>().name();
        cnch_table_name = engine_args[arg_num + 1]->as<ASTIdentifier &>().name();

        arg_num += 2;

        String date_column_name;

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setComment(args.comment);

        auto storage_settings = std::make_unique<MergeTreeSettings>(args.getContext()->getMergeTreeSettings());

        if (is_extended_storage_def)
        {
            ASTPtr partition_by_key;
            if (args.storage_def->partition_by)
                partition_by_key = args.storage_def->partition_by->ptr();

            /// Partition key may be undefined, but despite this we store it's empty
            /// value in partition_key structure. MergeTree checks this case and use
            /// single default partition with name "all".
            metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());

            if (args.storage_def->cluster_by)
                metadata.cluster_by_key = KeyDescription::getClusterByKeyFromAST(args.storage_def->cluster_by->ptr(), metadata.columns, args.getContext());

            /// PRIMARY KEY without ORDER BY is allowed and considered as ORDER BY.
            if (!args.storage_def->order_by && args.storage_def->primary_key)
                args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

            if (!args.storage_def->order_by)
                throw Exception(
                    "You must provide an ORDER BY or PRIMARY KEY expression in the table definition. "
                    "If you don't want this table to be sorted, use ORDER BY/PRIMARY KEY tuple()",
                    ErrorCodes::BAD_ARGUMENTS);

            /// Get sorting key from engine arguments.
            ///
            /// NOTE: store merging_param_key_arg as additional key column. We do it
            /// before storage creation. After that storage will just copy this
            /// column if sorting key will be changed.
            metadata.sorting_key = KeyDescription::getSortingKeyFromAST(
                args.storage_def->order_by->ptr(), metadata.columns, args.getContext(), std::nullopt);

            /// If primary key explicitly defined, than get it from AST
            if (args.storage_def->primary_key)
            {
                metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
            }
            else /// Otherwise we don't have explicit primary key and copy it from order by
            {
                metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.getContext());
                /// and set it's definition_ast to nullptr (so isPrimaryKeyDefined()
                /// will return false but hasPrimaryKey() will return true.
                metadata.primary_key.definition_ast = nullptr;
            }

            if (args.query.columns_list && args.query.columns_list->indices)
                for (auto & index : args.query.columns_list->indices->children)
                    metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(index, args.columns, args.getContext()));

            if (args.query.columns_list && args.query.columns_list->constraints)
                for (auto & constraint : args.query.columns_list->constraints->children)
                    metadata.constraints.constraints.push_back(constraint);

            if (args.query.columns_list && args.query.columns_list->unique)
                for (auto & unique_key : args.query.columns_list->unique->children)
                    metadata.unique_not_enforced.unique.push_back(unique_key);

            // For cnch, if storage_policy is not specified, modify it to cnch's default
            if (!args.storage_def->settings)
            {
                auto settings_ast = std::make_shared<ASTSetQuery>();
                settings_ast->is_standalone = false;
                args.storage_def->set(args.storage_def->settings, settings_ast);
            }

            if (!args.storage_def->settings->changes.tryGet("storage_policy"))
            {
                args.storage_def->settings->changes.push_back(
                    SettingChange("storage_policy", args.getContext()->getDefaultCnchPolicyName()));
            }

            storage_settings->loadFromQuery(*args.storage_def);

            // updates the default storage_settings with settings specified via SETTINGS arg in a query
            if (args.storage_def->settings)
                metadata.settings_changes = args.storage_def->settings->ptr();
        }

        DataTypes data_types = metadata.partition_key.data_types;
        if (!args.attach && !storage_settings->allow_floating_point_partition_key)
        {
            for (size_t i = 0; i < data_types.size(); ++i)
                if (isFloat(data_types[i]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Floating point partition key is not supported: {}", metadata.partition_key.column_names[i]);
        }

        /// In ANSI mode, allow_nullable_key must be true
        if (args.getLocalContext()->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
        {
            // If user sets allow_nullable_key=0.
            if (storage_settings->allow_nullable_key.changed && !storage_settings->allow_nullable_key.value)
                throw Exception("In ANSI mode, allow_nullable_key must be true.", ErrorCodes::BAD_ARGUMENTS);

            storage_settings->allow_nullable_key.value = true;
        }

        return std::make_shared<StorageDictCloudMergeTree>(
            args.table_id,
            cnch_database_name,
            cnch_table_name,
            metadata,
            args.getContext(),
            date_column_name,
            merging_params,
            std::move(storage_settings)
        );
    };

    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_replication = true,
        .supports_deduplication = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("DictCloudMergeTree", create, features);
}

}
