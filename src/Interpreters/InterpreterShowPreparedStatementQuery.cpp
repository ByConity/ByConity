#include <string>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InterpreterShowPreparedStatementQuery.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/PlanPrinter.h>

namespace DB
{
BlockIO InterpreterShowPreparedStatementQuery::execute()
{
    const auto * show_prepared = query_ptr->as<const ASTShowPreparedStatementQuery>();
    // get prepare from cache
    if (!context->getPreparedStatementManager())
        throw Exception("Prepare cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    if ((show_prepared->show_create || show_prepared->show_explain) && show_prepared->name.empty())
        throw Exception("No preapred statement name specified", ErrorCodes::LOGICAL_ERROR);

    std::ostringstream out;
    String result_column_name;
    auto * prepared_manager = context->getPreparedStatementManager();
    if (show_prepared->show_create)
    {
        auto prepared_object = prepared_manager->getObject(show_prepared->name);
        out << prepared_object.query;
        result_column_name = "Create Statement";
    }
    else if (show_prepared->show_explain)
    {
        auto prepared_object = prepared_manager->getObject(show_prepared->name);
        CTEInfo cte_info;
        for (auto & cte_it : prepared_object.cte_map)
            cte_info.add(cte_it.first, cte_it.second);
        QueryPlan plan(prepared_object.plan_root, cte_info, context->getPlanNodeIdAllocator());
        out << PlanPrinter::textLogicalPlan(plan, context, false, true, {});
        result_column_name = "Explain";
    }
    // else if (!show_prepared->name.empty())
    // {
    //     auto prepared_object = prepared_manager->getObject(show_prepared->name);
    //     CTEInfo cte_info;
    //     for (auto & cte_it : prepared_object.cte_map)
    //         cte_info.add(cte_it.first, cte_it.second);
    //     QueryPlan plan(prepared_object.plan_root, cte_info, context->getPlanNodeIdAllocator());
    //     out << "Prepare Name: " << show_prepared->name << "\n"
    //         << "Create Statement:\n"
    //         << prepared_object.query << ";\n"
    //         << "Explain:\n"
    //         << PlanPrinter::textLogicalPlan(plan, context, false, true, {});
    //     result_column_name = "Prepared Statement Detail";
    // }
    else
    {
        auto name_list = prepared_manager->getNames();
        for (auto & name : name_list)
            out << name << "\n";
        result_column_name = "Prepared Statement List";
    }

    BlockIO res;
    MutableColumnPtr binding_column = ColumnString::create();
    std::istringstream ss(out.str());
    std::string line;
    while (std::getline(ss, line))
        binding_column->insert(std::move(line));

    res.in
        = std::make_shared<OneBlockInputStream>(Block{{std::move(binding_column), std::make_shared<DataTypeString>(), result_column_name}});
    return res;
}

}
