/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <optional>
#include <Optimizer/Utils.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzers/TypeAnalyzer.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/ExpressionExtractor.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Storages/StorageDistributed.h>
#include <boost/math/special_functions/math_fwd.hpp>
#include <common/logger_useful.h>
#include "Parsers/IAST.h"

extern const char * build_version;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Utils
{

void assertIff(bool expression1, bool expression2)
{
    bool expression = (!(expression1) || (expression2)) && (!(expression2) || (expression1));
    if (!expression)
        throw Exception("Illegal State", ErrorCodes::LOGICAL_ERROR);
}

void checkState(bool expression)
{
    if (!expression)
    {
        throw Exception("Illegal State", ErrorCodes::LOGICAL_ERROR);
    }
}

void checkState(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception("Illegal State: " + msg, ErrorCodes::LOGICAL_ERROR);
    }
}

void checkArgument(bool expression)
{
    if (!expression)
    {
        throw Exception("Illegal Argument", ErrorCodes::LOGICAL_ERROR);
    }
}

void checkArgument(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception("Illegal Argument: " + msg, ErrorCodes::LOGICAL_ERROR);
    }
}

bool isIdentity(const String & symbol, const ConstASTPtr & expression) {
    return isIdentity(std::make_pair(symbol, expression));
}

bool isIdentity(const Assignment & assignment)
{
    String symbol = assignment.first;
    if (const auto * identifier = assignment.second->as<const ASTIdentifier>())
        return identifier->name() == symbol;
    return false;
}

bool isIdentity(const Assignments & assignments)
{
    return std::all_of(assignments.begin(), assignments.end(), [](const Assignment & assignment) {
        return isIdentity(assignment);
    });
}

bool isIdentity(const ProjectionStep & step)
{
    return !step.isFinalProject() && Utils::isIdentity(step.getAssignments());
}

bool isAlias(const Assignment & assignment)
{
    return assignment.second->getType() == ASTType::ASTIdentifier;
}

bool isAlias(const Assignments & assignments)
{
    return std::all_of(assignments.begin(), assignments.end(), [](const Assignment & assignment) { return isAlias(assignment); });
}

bool isIdentifierOrIdentifierCast(const ConstASTPtr & expression)
{
    if (const auto * function = expression->as<ASTFunction>())
    {
        return Poco::toLower(function->name) == "cast" && function->arguments->getChildren()[0]->getType() == ASTType::ASTIdentifier;
    }
    return expression->getType() == ASTType::ASTIdentifier;
}

ConstASTPtr tryUnwrapCast(const ConstASTPtr & expression, ContextMutablePtr context, const NamesAndTypes & names_and_types)
{
    if (const auto * function = expression->as<ASTFunction>();
        function && Poco::toLower(function->name) == "cast" && function->arguments->getChildren().size() == 2)
    {
        auto & source_expression = function->arguments->getChildren()[0];
        auto source_type = TypeAnalyzer::getType(source_expression, context, names_and_types);

        const auto * target_type_name = function->arguments->getChildren()[1]->as<ASTLiteral>();
        auto target_type = DataTypeFactory::instance().get(target_type_name->value.safeGet<String>());

        auto super_type = tryGetLeastSupertype(DataTypes{source_type, target_type});
        if (super_type != nullptr && super_type->equals(*target_type))
        {
            return source_expression;
        }
    }
    return expression;
}

NameToNameMap extractIdentities(const ProjectionStep & project)
{
    NameToNameMap result;
    for (const auto & assignment: project.getAssignments())
        if (auto identifier = assignment.second->as<const ASTIdentifier>())
            result.emplace(assignment.first, identifier->name());
    return result;
}

std::unordered_map<String, String> computeIdentityTranslations(const Assignments & assignments)
{
    std::unordered_map<String, String> output_to_input;
    for (const auto & assignment : assignments)
    {
        if (const auto * identifier = assignment.second->as<ASTIdentifier>())
        {
            output_to_input[assignment.first] = identifier->name();
        }
    }
    return output_to_input;
}

ASTPtr extractAggregateToFunction(const AggregateDescription & aggregate_description)
{
    const auto function = std::make_shared<ASTFunction>();
    function->name = aggregate_description.function->getName();
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);
    for (auto & argument : aggregate_description.argument_names)
        function->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(argument));
    if (!aggregate_description.parameters.empty())
    {
        function->parameters = std::make_shared<ASTExpressionList>();
        for (auto & parameter : aggregate_description.parameters)
            function->parameters->children.emplace_back(std::make_shared<ASTLiteral>(parameter));
    }
    return function;
}

bool containsAggregateFunction(const ASTPtr & ast)
{
    if (auto function = ast->as<ASTFunction>())
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
            return true;
    for (const auto & child : ast->children)
        if (containsAggregateFunction(child))
            return true;
    return false;
}


bool canIgnoreNullsDirection(const DataTypePtr & type)
{
    return !type->isNullable() && type->getTypeId() != TypeIndex::Float32 && type->getTypeId() != TypeIndex::Float64;
}

bool checkFunctionName(const ASTFunction & function, const String & expect_name)
{
    if (function.name == expect_name)
        return true;

    auto res = FunctionFactory::instance().getCanonicalName(function.name);

    if (res)
    {
        auto & canonical_name = *res;
        return canonical_name == expect_name ||
            (FunctionFactory::instance().isCaseInsensitive(canonical_name) && canonical_name == Poco::toLower(expect_name));
    }

    return false;
}

bool ConstASTPtrOrdering::operator()(const ConstASTPtr & predicate_1, const ConstASTPtr & predicate_2) const
{
    size_t symbol_size_1 = SymbolsExtractor::extract(predicate_1).size();
    size_t symbol_size_2 = SymbolsExtractor::extract(predicate_2).size();
    if (symbol_size_1 != symbol_size_2)
        return symbol_size_1 < symbol_size_2;

    size_t sub_expression_size_1 = SubExpressionExtractor::extract(predicate_1).size();
    size_t sub_expression_size_2 = SubExpressionExtractor::extract(predicate_2).size();
    if (sub_expression_size_1 != sub_expression_size_2)
        return sub_expression_size_1 < sub_expression_size_2;

    return predicate_1->getColumnName() < predicate_2->getColumnName();
}

//Determine whether it is NAN
bool isFloatingPointNaN(const DataTypePtr & type, const Field & value)
{
    TypeIndex type_id = type->getTypeId();

    if (type_id == TypeIndex::Float32)
        return std::isnan(value.get<Float64>());

    if (type_id == TypeIndex::Float64)
        return std::isnan(value.get<Float64>());

    return false;
}

String flipOperator(const String & name)
{
    if (name == "equals")
        return name;
    if (name == "notEquals")
        return name;
    if (name == "less")
        return "greater";
    if (name == "lessOrEquals")
        return "greaterOrEquals";
    if (name == "greater")
        return "less";
    if (name == "greaterOrEquals")
        return "lessOrEquals";

    throw Exception("Unsupported comparison", DB::ErrorCodes::LOGICAL_ERROR);
}

bool canChangeOutputRows(const Assignments & assignments, ContextPtr context)
{
    for (const auto & assignment: assignments)
        if (ExpressionDeterminism::canChangeOutputRows(assignment.second, context))
            return true;

    return false;
}

bool canChangeOutputRows(const ProjectionStep & project, ContextPtr context)
{
    return canChangeOutputRows(project.getAssignments(), context);
}

static void extractNameToTypeImpl(PlanNodeBase * node, std::optional<NameToType> & res)
{
    if (node && res)
    {
        for (const auto & item : node->getCurrentDataStream().header)
        {
            const auto & name = item.name;
            const auto & type = item.type;
            if (auto it = res->find(name); it != res->end() && !it->second->equals(*type))
            {
                res = std::nullopt;
                break;
            }

            res->emplace(name, type);
        }
    }

    if (res)
    {
        for (auto & child : node->getChildren())
            extractNameToTypeImpl(child.get(), res);
    }
}


std::optional<NameToType> extractNameToType(const PlanNodeBase & node)
{
    std::optional<NameToType> res = NameToType{};
    extractNameToTypeImpl(const_cast<PlanNodeBase *>(&node), res);
    return res;
}

std::string getVersionFromSystem()
{
    if(build_version != nullptr && build_version[0] != '\0')
        return std::string(build_version);
    return "";
}
}
}
