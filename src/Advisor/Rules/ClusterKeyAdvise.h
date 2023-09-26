#pragma once

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/Types.h>
#include <Poco/Logger.h>

namespace DB
{

class ClusterKeyAdvisor : public IWorkloadAdvisor
{
public:
    String getName() const override { return "ClusterKeyAdvisor"; }
    WorkloadAdvises analyze(AdvisorContext & context) const override;

private:
    bool isValidColumn(const QualifiedColumnName & column, AdvisorContext & context) const;
    Poco::Logger * log = &Poco::Logger::get("ClusterKeyAdvisor");
};

}
