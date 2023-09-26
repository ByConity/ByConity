#pragma once

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>

namespace DB
{

class IndexAdvise : public IWorkloadAdvisor
{
public:
    WorkloadAdvises analyze(AdvisorContext & context) const override { return {}; }
};

}
