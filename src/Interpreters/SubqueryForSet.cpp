#include <Interpreters/SubqueryForSet.h>
#include <QueryPlan/QueryPlan.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

SubqueryForSet::SubqueryForSet() = default;
SubqueryForSet::~SubqueryForSet() = default;
SubqueryForSet::SubqueryForSet(SubqueryForSet &&) = default;
SubqueryForSet & SubqueryForSet::operator= (SubqueryForSet &&) = default;

}
