#pragma once

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/Types.h>
#include <Poco/Logger.h>

namespace DB
{

class ColumnUsageAdvisor : public IWorkloadAdvisor
{
public:
    String getName() const override { return "ColumnUsageAdvisor"; }
    WorkloadAdvises analyze(AdvisorContext & context) const override;
    
private:
    // Poco::Logger * log = getLogger("OrderByKeyAdvisor");
};

}
