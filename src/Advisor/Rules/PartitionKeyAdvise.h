#pragma once

#include <Common/Logger.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/AdvisorContext.h>
#include <Advisor/WorkloadTable.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/Types.h>
#include <Poco/Logger.h>

#include <vector>

namespace DB
{

class PartitionKeyAdvisor : public IWorkloadAdvisor
{
public:
    String getName() const override { return "PartitionKeyAdvisor"; }

    WorkloadAdvises analyze(AdvisorContext & context) const override;

    WorkloadAdvises frequencyBasedAdvise(AdvisorContext & context) const;

    WorkloadAdvises memoBasedAdvise(AdvisorContext & context) const;

private:
    std::vector<QualifiedColumnName> getSortedInterestingColumns(AdvisorContext & context) const;
    bool isValidColumn(const QualifiedColumnName & column, AdvisorContext & context) const;

    LoggerPtr log = getLogger("PartitionKeyAdvisor");

    static constexpr bool enable_memo_based_advise = 1;
};


}
