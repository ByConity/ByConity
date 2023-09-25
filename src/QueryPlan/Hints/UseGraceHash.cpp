#include <QueryPlan/Hints/PlanHintFactory.h>
#include <QueryPlan/Hints/UseGraceHash.h>


namespace DB
{

UseGraceHash::UseGraceHash(const SqlHint & sql_hint, const ContextMutablePtr &) : JoinAlgorithmHints(sql_hint.getOptions())
{
}

String UseGraceHash::getName() const
{
    return name;
}

void registerHintUseGraceHash(PlanHintFactory & factory)
{
    factory.registerPlanHint(UseGraceHash::name, &UseGraceHash::create, PlanHintFactory::CaseInsensitive);
}

}
