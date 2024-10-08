#pragma once

#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadTable.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAdviseQuery.h>
#include <Poco/Logger.h>

#include <vector>

namespace DB
{

class Advisor
{
public:
    explicit Advisor(ASTAdviseQuery::AdvisorType type_) : type(type_)
    {
    }
    WorkloadAdvises analyze(const std::vector<String> & queries, ContextPtr context);

    private:
    static WorkloadAdvisors getAdvisors(ASTAdviseQuery::AdvisorType type);

    ASTAdviseQuery::AdvisorType type;
    LoggerPtr log = getLogger("Advisor");
};

}
