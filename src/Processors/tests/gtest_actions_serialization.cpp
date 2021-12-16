#include <gtest/gtest.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/ActionsDAG.h>
#include <DataTypes/DataTypeFactory.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

ActionsDAGPtr createActionsColumn()
{
    auto actions_dag = std::make_shared<ActionsDAG>();

    ColumnWithTypeAndName column;
    column.name = "TEST";

    DataTypePtr type = DataTypeFactory::instance().get("String");
    column.column = type->createColumnConst(1, Field("test constant"));
    column.type = type;

    actions_dag->addColumn(column);
    return actions_dag;
}

ActionsDAGPtr createActionsFunction()
{
    auto actions_dag = std::make_shared<ActionsDAG>();
    const auto & context = getContext().context;

    tryRegisterFunctions();    
    auto & factory = FunctionFactory::instance();
    auto function_builder = factory.get("lower", context);

    ColumnWithTypeAndName column;
    column.name = "TEST";

    DataTypePtr type = DataTypeFactory::instance().get("String");
    column.column = type->createColumnConst(1, Field("TEST CONSTANT"));
    column.type = type;

    actions_dag->addColumn(column);

    ActionsDAG::NodeRawConstPtrs children;
    children.push_back(&actions_dag->getNodes().back());
    actions_dag->addFunction(function_builder, std::move(children), "lower()");

    return actions_dag;
}

ActionsDAGPtr serializeActions(const ActionsDAGPtr & actions_dag)
{
    /**
     * serialize to buffer
     */
    WriteBufferFromOwnString write_buffer;
    actions_dag->serialize(write_buffer);

    /**
     * deserialize from buffer
     */
    const auto & context = getContext().context;

    ReadBufferFromString read_buffer(write_buffer.str());
    return ActionsDAG::deserialize(read_buffer, context);
}

void checkResult(const ActionsDAGPtr & lhs, const ActionsDAGPtr & rhs)
{
    std::cout<< "<< lhs << \n" << lhs->dumpDAG()<<std::endl;
    std::cout<< "<< rhs << \n" << rhs->dumpDAG()<<std::endl;
    EXPECT_EQ(lhs->dumpDAG(), rhs->dumpDAG());
}

TEST(TestActions, TestActionsSerialization)
{
    auto column_actions_dag = createActionsColumn();
    auto new_column_actions_dag = serializeActions(column_actions_dag);
    checkResult(column_actions_dag, new_column_actions_dag);

    auto function_actions_dag = createActionsFunction();
    auto new_function_actions_dag = serializeActions(function_actions_dag);
    checkResult(function_actions_dag, new_function_actions_dag); 
}
