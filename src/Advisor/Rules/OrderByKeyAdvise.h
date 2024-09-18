#pragma once

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <Poco/Logger.h>

namespace DB
{

class OrderByKeyAdvisor : public IWorkloadAdvisor
{
public:
    String getName() const override { return "OrderByKeyAdvisor"; }
    WorkloadAdvises analyze(AdvisorContext & context) const override;

private:
    bool isValidColumn(const QualifiedColumnName & column, AdvisorContext & context) const;
    LoggerPtr log = getLogger("ClusterKeyAdvisor");
};

}
