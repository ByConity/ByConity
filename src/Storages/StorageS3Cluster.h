#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <memory>
#include <optional>

#include <common/shared_ptr_helper.h>

#include "Client/Connection.h"
#include <Interpreters/Cluster.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3.h>

namespace DB
{

class Context;

class StorageS3Cluster : public shared_ptr_helper<StorageS3Cluster>, public IStorage
{
    friend struct shared_ptr_helper<StorageS3Cluster>;
public:
    std::string getName() const override { return "S3Cluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, unsigned /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageS3Cluster(
        const String & cluster_name_,
        const StorageS3::Configuration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);
    
    void updateConfigurationIfChanged(ContextPtr local_context);

private:

    // void addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context) override;

    String cluster_name;
    StorageS3::Configuration s3_configuration;

    /// Connections from initiator to other nodes
    std::vector<std::shared_ptr<Connection>> connections;

    NamesAndTypesList virtual_columns;
    Block virtual_block;
};


}

#endif
