#pragma once

#include <Interpreters/StorageID.h>
#include <Core/Types.h>
#include <memory>

namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/**
 * Metrics used in optimizer per query.
 */
class OptimizerMetrics
{
public:
    void addMaterializedView(const StorageID & view) { used_materialized_views.emplace_back(view); }
    const std::vector<StorageID> & getUsedMaterializedViews() const { return used_materialized_views; }

private:
    std::vector<StorageID> used_materialized_views;
};

using OptimizerMetricsPtr = std::shared_ptr<OptimizerMetrics>;
}
