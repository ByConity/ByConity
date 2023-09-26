#pragma once

#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadTable.h>
#include <Core/Types.h>
#include <Parsers/ASTAdviseQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>

#include <vector>

namespace DB
{

class Advisor
{
public:
    explicit Advisor(ASTAdviseQuery::AdvisorType type);

    WorkloadAdvises analyze(const std::vector<String> & queries, WorkloadTables & tables, ContextPtr context);
private:
    WorkloadAdvisors advisors;
    Poco::Logger * log = &Poco::Logger::get("Advisor");
};

}
