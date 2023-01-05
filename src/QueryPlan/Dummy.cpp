#include <QueryPlan/ValuesStep.h>
#include <QueryPlan/Dummy.h>
#include <Core/NamesAndTypes.h>

namespace DB 
{

std::pair<String, PlanNodePtr> createDummyPlanNode(ContextMutablePtr context)
{
    auto symbol = context->getSymbolAllocator()->newSymbol("dummy");
    NamesAndTypes header;
    Fields data;

    header.emplace_back(symbol, std::make_shared<DataTypeUInt8>());
    data.emplace_back(0U);

    auto values_step = std::make_shared<ValuesStep>(header, data);
    auto node = PlanNodeBase::createPlanNode(context->nextNodeId(), values_step);
    return {std::move(symbol), std::move(node)};
}
}
