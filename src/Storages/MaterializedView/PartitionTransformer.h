#pragma once

#include <Common/Logger.h>
#include <common/shared_ptr_helper.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Protos/data_models_mv.pb.h>

namespace DB
{
struct MaterializedViewStructure;
using MaterializedViewStructurePtr = std::shared_ptr<MaterializedViewStructure>;

using VersionPart = Protos::VersionedPartition;
using VersionPartPtr = std::shared_ptr<VersionPart>;
using VersionPartPtrs = std::vector<VersionPartPtr>;

using VersionPartContainerPtr = std::shared_ptr<Protos::VersionedPartitions>;
using VersionPartContainerPtrs = std::vector<VersionPartContainerPtr>;

/// target partition name --> source partition name list
using PartMapRelations = std::unordered_map<String, std::set<String>>;

/// Partitition difference between previous and current snapshot
struct PartitionDiff
{
    bool paritition_based_refresh {true};
    StorageID depend_storage_id = StorageID::createEmpty();
    VersionPartPtrs add_partitions;
    VersionPartPtrs drop_partitions;
    std::unordered_map<String, String> part_name_to_binary;
    static VersionPartContainerPtrs generatePartitionContainer(const VersionPartPtrs & partitions);
    void findPartitions(std::set<String> & partition_names, std::shared_ptr<PartitionDiff> & dest_part_diff);
    String toString() const;
};

using PartitionDiffPtr = std::shared_ptr<PartitionDiff>;

/// Async refreh task parameters
struct AsyncRefreshParam
{
    String drop_partition_query;
    String insert_select_query;
    String insert_overwrite_query;
    PartitionDiffPtr part_diff;
    PartMapRelations part_relation;
    bool partition_refresh;
    String getPartitionMap();
};

using AsyncRefreshParamPtr = std::shared_ptr<AsyncRefreshParam>;
using AsyncRefreshParamPtrs = std::vector<AsyncRefreshParamPtr>;

/// Dependent base table information
struct BaseTableInfo
{
    size_t filter_pos = std::numeric_limits<size_t>::max();
    ExpressionActionsPtr transform_expr;
    ASTPtr partition_key_ast;
    StoragePtr storage;
    size_t unique_id = 0;
};

using BaseTableInfoPtr = std::shared_ptr<BaseTableInfo>;

/// Partition trasnfomer is used to calculate target table partition list from source table partition 
class PartitionTransformer
{
public:
    explicit PartitionTransformer(ASTPtr mv_query_, const StorageID & target_id, bool async_materialized_view_)
        : mv_query(mv_query_), target_table_id(target_id), async_materialized_view(async_materialized_view_), log(getLogger("PartitionTransformer")) {}

    void validate(ContextMutablePtr local_context);
    void validate(ContextMutablePtr local_context, MaterializedViewStructurePtr structure);
    std::unordered_set<StorageID> & getNonDependBaseTables() { return non_depend_base_tables; }
    std::unordered_map<StorageID, BaseTableInfoPtr> & getDependBaseTables() { return depend_base_tables; }
    std::unordered_set<StoragePtr> & getBaseTables() { return base_tables; }
    StoragePtr & getTargeTable() { return target_table; }
    BaseTableInfoPtr getBaseTableInfo(const StorageID & base_table_id);
    AsyncRefreshParamPtrs constructRefreshParams(PartMapRelations & part_map, PartitionDiffPtr & part_diff, bool combine_params, bool partition_refresh,
                                        ContextMutablePtr local_context);
    PartMapRelations transform(const VersionPartPtrs & ver_partitions, std::unordered_map<String, String> & name_to_binary, const StorageID & base_table_id);
    static VersionPartPtr convert(const StoragePtr & storage, const Protos::LastModificationTimeHint & part_last_update);
    static String parsePartitionKey(const StoragePtr & storage, String partition_key_binary);
    bool alwaysNonPartitionRefresh() const { return always_non_partition_based; }

private:
    ASTPtr mv_query;
    StorageID target_table_id = StorageID::createEmpty();
    bool async_materialized_view = false;
    StoragePtr target_table;
    ASTPtr target_partition_key_ast;

    // no concurrent control
    bool validated = false;

    // non depend base table id
    std::unordered_set<StorageID> non_depend_base_tables;

    /// depend_base_table_id -> table_info
    std::unordered_map<StorageID, BaseTableInfoPtr> depend_base_tables;

    /// all base table
    std::unordered_set<StoragePtr> base_tables;
    LoggerPtr log;
    
    /// matererialized view sql is always non partition based 
    bool always_non_partition_based = true;
};

using PartitionTransformerPtr = std::shared_ptr<PartitionTransformer>;

}
