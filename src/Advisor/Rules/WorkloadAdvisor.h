#pragma once

#include <Advisor/AdvisorContext.h>
#include <Advisor/WorkloadTable.h>

#include <Core/QualifiedTableName.h>
#include <Core/Types.h>

#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{
class IWorkloadAdvise;
using WorkloadAdvisePtr = std::shared_ptr<IWorkloadAdvise>;
using WorkloadAdvises = std::vector<WorkloadAdvisePtr>;

/**
 * The Workload Tuning Advisor is a tool that can help you significantly improve your workload performance.
 * The task of selecting which indexes, materialized views, clustering keys, or database partitions to create
 * for a complex workload can be daunting. The Workload Tuning Advisor identifies all of the objects that are
 * needed to improve the performance of your workload.
 *
 * Given a set of SQL statements in a workload, the Workload Tuning Advisor generates recommendations for:
 *
 * New indexes
 * New materialized views
 * New clustering key
 * Conversion to multidimensional clustering key tables
 * The redistribution of tables
 *
 * The Workload Tuning Advisor can implement some or all of these recommendations immediately, or you can
 * schedule them to run at a later time. Use the advis command to launch the Workload Tuning Advisor utility.
 *
 * The Workload Tuning Advisor can help simplify the following tasks:
 *
 * Planning for and setting up a new database
 *
 * While designing your database, use the Workload Tuning Advisor to generate design alternatives in a test
 * environment for indexing, MVs, table clustering keys, or database partitioning.
 *
 * In partitioned database environments, you can use the Workload Tuning Advisor to:
 *
 * - Determine an appropriate database partitioning strategy before loading data into a database
 * - Assist in upgrading from a single-partition database to a multi-partition database
 * - Assist in migrating from another database product to a multi-partition Db2 database
 *
 * Workload performance tuning
 *
 * After your database is set up, you can use the Workload Tuning Advisor to:
 *
 * - Improve the performance of a particular statement or workload
 */
class IWorkloadAdvisor
{
public:
    virtual ~IWorkloadAdvisor() = default;
    virtual String getName() const = 0;
    virtual WorkloadAdvises analyze(AdvisorContext & context) const = 0;
};
using WorkloadAdvisorPtr = std::shared_ptr<IWorkloadAdvisor>;
using WorkloadAdvisors = std::vector<WorkloadAdvisorPtr>;

class IWorkloadAdvise
{
public:
    virtual ~IWorkloadAdvise() = default;

    /**
     * return error message if failed.
     */
    virtual String apply(WorkloadTables & tables) = 0;

    virtual QualifiedTableName getTable() = 0;
    virtual std::optional<String> getColumnName() { return {}; }
    virtual String getAdviseType() = 0;
    virtual String getOriginalValue() = 0;
    virtual String getOptimizedValue() = 0;
    virtual double getBenefit() { return 0.0; }
    virtual std::vector<std::pair<String, double>> getCandidates() { return {}; }
    virtual std::vector<String> getRelatedQueries()
    {
        return {};
    }
};

}
