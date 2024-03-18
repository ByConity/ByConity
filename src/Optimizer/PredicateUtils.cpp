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

#include <Analyzers/ASTEquals.h>
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/ProjectionStep.h>

#include <common/arithmeticOverflow.h>

namespace DB
{

namespace
{
    template <typename T>
    ASTPtr castToASTPtr(T && p)
    {
        using TP = std::decay_t<decltype(p)>;

        if constexpr (std::is_same_v<TP, ASTPtr>)
            return p; // employ RVO?
        else if constexpr (std::is_same_v<TP, ConstASTPtr>)
            return std::const_pointer_cast<IAST>(std::forward<TP>(p));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "logical errror!");
    }
}

bool PredicateUtils::equals(ASTPtr & p1, ASTPtr & p2)
{
    return ASTEquality::compareTree(p1, p2);
}

bool PredicateUtils::equals(ConstASTPtr & p1, ConstASTPtr & p2)
{
    return ASTEquality::compareTree(p1, p2);
}

bool PredicateUtils::equals(ConstHashAST & p1, ConstHashAST & p2)
{
    return EqAST::Equal()(p1, p2);
}

template <typename T, enable_if_ast<T>>
std::vector<T> PredicateUtils::extractConjuncts(T predicate)
{
    std::vector<T> result;
    extractPredicate(predicate, PredicateConst::AND, result);
    return result;
}

template <typename T, enable_if_ast<T>>
std::vector<T> PredicateUtils::extractDisjuncts(T predicate)
{
    std::vector<T> result;
    extractPredicate(predicate, PredicateConst::OR, result);
    return result;
}

template <typename T, enable_if_ast<T>>
std::vector<T> PredicateUtils::extractPredicate(T predicate)
{
    std::vector<T> result;
    const auto & fun = predicate->template as<const ASTFunction &>();
    extractPredicate(predicate, fun.name, result);
    return result;
}

std::vector<std::vector<ConstASTPtr>> PredicateUtils::extractSubPredicates(ConstASTPtr predicate)
{
    std::vector<std::vector<ConstASTPtr>> sub_predicates;
    std::vector<ConstASTPtr> predicates = extractPredicate(predicate);
    for (const ConstASTPtr & sub_predicate : predicates)
    {
        if (sub_predicate && sub_predicate->as<const ASTFunction>())
        {
            const auto & fun = sub_predicate->as<const ASTFunction &>();
            if (fun.name == PredicateConst::AND || fun.name == PredicateConst::OR)
            {
                std::vector<ConstASTPtr> sub = extractPredicate(sub_predicate);
                sub_predicates.emplace_back(sub);
            }
            else
            {
                std::vector<ConstASTPtr> sub{sub_predicate};
                sub_predicates.emplace_back(sub);
            }
        }
        else
        {
            std::vector<ConstASTPtr> sub{sub_predicate};
            sub_predicates.emplace_back(sub);
        }
    }
    return sub_predicates;
}

ConstASTPtr PredicateUtils::extractCommonPredicates(ConstASTPtr predicate, ContextMutablePtr & context)
{
    auto fun = predicate->as<const ASTFunction &>();
    std::vector<std::vector<ConstASTPtr>> sub_predicates = extractSubPredicates(predicate);

    std::unordered_map<String, ConstASTPtr> all_predicate_map;
    std::unordered_map<size_t, std::vector<std::pair<ConstASTPtr, String>>> sub_predicates_map;
    std::vector<std::set<ConstASTPtr>> deterministic_sub_predicates;
    for (size_t i = 0; i < sub_predicates.size(); ++i)
    {
        std::vector<std::pair<ConstASTPtr, String>> pairs;
        for (auto & sub : sub_predicates[i])
        {
            all_predicate_map[sub->getColumnName()] = sub;
            pairs.emplace_back(std::make_pair(sub, sub->getColumnName()));
        }
        sub_predicates_map[i] = pairs;

        std::set<ConstASTPtr> filtered = ExpressionDeterminism::filterDeterministicPredicates(sub_predicates[i], context);
        deterministic_sub_predicates.emplace_back(filtered);
    }

    std::vector<std::set<String>> deterministic_sub_predicates_string;
    for (auto & sub_predicate : deterministic_sub_predicates)
    {
        std::set<String> sub_predicate_names;
        for (const auto & sub : sub_predicate)
        {
            sub_predicate_names.emplace(sub->getColumnName());
        }
        deterministic_sub_predicates_string.emplace_back(sub_predicate_names);
    }
    // extract common predicate from sub predicates.
    std::set<String> common_predicates;
    std::set<String> first = deterministic_sub_predicates_string[0];
    for (const auto & first_predicate : first)
    {
        bool all_contains = true;
        for (size_t i = 1; i < deterministic_sub_predicates_string.size(); ++i)
        {
            std::set<String> sub_predicate = deterministic_sub_predicates_string[i];
            if (!sub_predicate.contains(first_predicate))
            {
                all_contains = false;
                break;
            }
        }
        if (all_contains)
        {
            common_predicates.emplace(first_predicate);
        }
    }

    std::vector<std::vector<ConstASTPtr>> uncorrelated_sub_predicates;
    for (size_t i = 0; i < sub_predicates.size(); ++i)
    {
        std::vector<std::pair<ConstASTPtr, String>> & pairs = sub_predicates_map[i];
        std::vector<std::pair<ConstASTPtr, String>> uncorrelated_map = removeAll(pairs, common_predicates);

        std::vector<ConstASTPtr> uncorrelate_predicates;
        for (auto & uncorrelated : uncorrelated_map)
        {
            uncorrelate_predicates.emplace_back(uncorrelated.first);
        }
        uncorrelated_sub_predicates.emplace_back(uncorrelate_predicates);
    }

    String fun_name_flipped = flip(fun.name);

    std::vector<ConstASTPtr> uncorrelated_predicates;
    for (auto & uncorrelated_sub_predicate : uncorrelated_sub_predicates)
    {
        auto combined = PredicateUtils::combinePredicates(fun_name_flipped, uncorrelated_sub_predicate);
        uncorrelated_predicates.emplace_back(combined);
    }

    auto combined_uncorrelated_predicates = PredicateUtils::combinePredicates(fun.name, uncorrelated_predicates);

    std::vector<ConstASTPtr> all;
    for (const auto & common : common_predicates)
    {
        all.emplace_back(all_predicate_map[common]);
    }
    all.emplace_back(combined_uncorrelated_predicates);
    return PredicateUtils::combinePredicates(fun_name_flipped, all);
}

ConstASTPtr PredicateUtils::distributePredicate(ConstASTPtr or_predicate, ContextMutablePtr & context)
{
    const auto & or_fun = or_predicate->as<const ASTFunction &>();
    if (!ExpressionDeterminism::isDeterministic(or_predicate, context))
    {
        // Do not distribute boolean expressions if there are any non-deterministic elements
        // TODO: This can be optimized further if non-deterministic elements are not repeated
        return or_predicate;
    }
    std::vector<std::vector<ConstASTPtr>> sub_predicates = extractSubPredicates(or_predicate);
    std::vector<std::vector<ConstASTPtr>> sub_predicates_to_set;
    for (auto & sub : sub_predicates)
    {
        EqualityASTSet distinct;
        std::vector<ConstASTPtr> sets;
        for (auto & predicate : sub)
        {
            if (distinct.emplace(predicate).second)
            {
                sets.emplace_back(predicate);
            }
        }
        sub_predicates_to_set.emplace_back(sets);
    }
    Int64 original_base_expressions = 0;
    for (auto & set : sub_predicates_to_set)
    {
        original_base_expressions += set.size();
    }

    Int64 new_base_expressions = 1;
    bool overflow = false;

    for (auto & set : sub_predicates_to_set)
    {
        Int64 size = set.size();
        overflow |= common::mulOverflow(new_base_expressions, size, new_base_expressions);
    }
    overflow |= common::mulOverflow(new_base_expressions, Int64(sub_predicates_to_set.size()), new_base_expressions);
    if (overflow)
    {
        // Integer overflow from multiplication means there are too many expressions
        return or_predicate;
    }
    if (new_base_expressions > original_base_expressions * 2)
    {
        // Do not distribute boolean expressions if it would create 2x more base expressions
        // (e.g. A, B, C, D from the above example). This is just an arbitrary heuristic to
        // avoid cross product expression explosion.
        return or_predicate;
    }
    std::vector<std::vector<ConstASTPtr>> cross_product = cartesianProduct(sub_predicates_to_set);
    std::vector<ConstASTPtr> combined_cross_product;
    for (const auto & produce : cross_product)
    {
        auto combined = PredicateUtils::combinePredicates(or_fun.name, produce);
        combined_cross_product.emplace_back(combined);
    }
    return PredicateUtils::combinePredicates(flip(or_fun.name), combined_cross_product);
}

bool compareASTPtr(ASTPtr & left, ASTPtr & right)
{
    auto left_name = left->getColumnName();
    auto right_name = right->getColumnName();
    return MurmurHash3Impl64::apply(left_name.c_str(), left_name.size()) < MurmurHash3Impl64::apply(right_name.c_str(), right_name.size());
}

template <bool flatten, typename T, enable_if_ast<T>>
ASTPtr PredicateUtils::combineConjuncts(const std::vector<T> & predicates)
{
    if (predicates.empty())
    {
        return PredicateConst::TRUE_VALUE;
    }

    ASTSet<T> distinct; // todo: templatize EqualityASTSet
    std::vector<ASTPtr> conjuncts;
    for (const auto & predicate : predicates)
    {
        assert(predicate.get() && "predicate can't be null");
        if constexpr (flatten)
        {
            std::vector<T> extract_predicates = extractConjuncts<T>(predicate);
            for (auto & extract : extract_predicates)
            {
                if (!isTruePredicate(extract) && distinct.emplace(extract).second)
                {
                    conjuncts.emplace_back(extract->clone()); // TODO: remove clone, check tpcds correctness
                }
            }
        }
        else
        {
            if (!isTruePredicate(predicate) && distinct.emplace(predicate).second)
            {
                conjuncts.emplace_back(predicate->clone()); // TODO: remove clone, check tpcds correctness
            }
        }
    }

    for (const auto & conjunct : conjuncts)
    {
        if (isFalsePredicate(conjunct))
        {
            return PredicateConst::FALSE_VALUE;
        }
    }

    if (conjuncts.empty())
    {
        return PredicateConst::TRUE_VALUE;
    }

    if (conjuncts.size() == 1)
    {
        return conjuncts[0];
    }

    return makeASTFunction(PredicateConst::AND, conjuncts);
}

template <bool flatten, typename T, enable_if_ast<T>>
ASTPtr PredicateUtils::combineDisjuncts(const std::vector<T> & predicates)
{
    return combineDisjunctsWithDefault<flatten>(predicates, PredicateConst::FALSE_VALUE);
}

template <bool /* flatten */, typename T, enable_if_ast<T>>
ASTPtr PredicateUtils::combineDisjunctsWithDefault(const std::vector<T> & predicates, const ASTPtr & default_ast)
{
    if (predicates.empty())
        return default_ast;
    if (predicates.size() == 1)
        return predicates[0]->clone(); // TODO: remove clone, check tpcds correctness

    ASTSet<T> distinct; // todo: templatize EqualityASTSet
    std::vector<ASTPtr> args;
    for (auto & arg : predicates)
    {
        if (isTruePredicate(arg))
            return PredicateConst::TRUE_VALUE;
        if (isFalsePredicate(arg))
            continue;
        if (distinct.emplace(arg).second)
            args.emplace_back(arg->clone()); // TODO: remove clone, check tpcds correctness
    }

    if (args.empty())
        return default_ast;
    if (args.size() == 1)
        return args[0];

    return makeASTFunction(PredicateConst::OR, args);
}

template <bool flatten, typename T, enable_if_ast<T>>
ASTPtr PredicateUtils::combinePredicates(const String & fun, std::vector<T> predicates)
{
    if (fun == PredicateConst::AND)
    {
        return combineConjuncts<flatten>(predicates);
    }
    return combineDisjuncts<flatten>(predicates);
}

template <typename T, enable_if_ast<T>>
bool PredicateUtils::isTruePredicate(const T & predicate)
{
    if (const auto * literal = predicate->template as<const ASTLiteral>())
    {
        if (literal->getColumnName() == "1")
        {
            return true;
        }
    }
    return false;
}

template <typename T, enable_if_ast<T>>
bool PredicateUtils::isFalsePredicate(const T & predicate)
{
    if (const auto * literal = predicate->template as<const ASTLiteral>())
    {
        if (literal->value.isNull() || literal->getColumnName() == "0")
        {
            return true;
        }
    }
    return false;
}

bool PredicateUtils::containsAll(const Strings & partition_symbols, const std::set<String> & unique_symbols)
{
    bool contains = true;
    for (const auto & unique : unique_symbols)
    {
        if (std::find(partition_symbols.begin(), partition_symbols.end(), unique) == partition_symbols.end())
        {
            /* does not contain */
            contains = false;
            break;
        }
    }
    return contains;
}

bool PredicateUtils::isInliningCandidate(ConstASTPtr & predicate, ProjectionNode & node)
{
    // candidate symbols for inlining are
    //   1. references to simple constants or symbol references
    //   2. references to complex expressions that appear only once
    // which come from the node, as opposed to an enclosing scope.
    std::set<String> child_output_set;
    for (const auto & output : node.getStep()->getOutputStream().header)
    {
        child_output_set.emplace(output.name);
    }
    std::unordered_map<String, UInt64> dependencies;
    for (const auto & symbol : SymbolsExtractor::extract(predicate))
    {
        if (child_output_set.contains(symbol))
        {
            if (dependencies.contains(symbol))
            {
                UInt64 & count = dependencies[symbol];
                count++;
            }
            else
            {
                dependencies[symbol] = 1;
            }
        }
    }

    const auto & step = *node.getStep();
    auto assignments = step.getAssignments();

    bool all_match = true;
    for (auto & dependency : dependencies)
    {
        String symbol = dependency.first;
        UInt64 count = dependency.second;

        bool symbol_reference_or_literal = false;


        auto & expr = assignments.at(symbol);
        if (expr->as<const ASTLiteral>() || expr->as<const ASTIdentifier>())
        {
            symbol_reference_or_literal = true;
        }

        if (!(count == 1 || symbol_reference_or_literal))
        {
            all_match = false;
        }
    }
    return all_match;
}

ASTPtr PredicateUtils::extractJoinPredicate(JoinNode & node)
{
    const auto & step = *node.getStep();
    const Names & left_keys = step.getLeftKeys();
    const Names & right_keys = step.getRightKeys();
    if (left_keys.empty() && right_keys.empty() && PredicateUtils::isTruePredicate(step.getFilter()))
    {
        return PredicateConst::TRUE_VALUE;
    }

    ASTs join_predicates;
    for (size_t i = 0; i < left_keys.size(); ++i)
    {
        ASTPtr join_predicate = makeASTFunction(
            "equals", ASTs{std::make_shared<ASTIdentifier>(left_keys.at(i)), std::make_shared<ASTIdentifier>(right_keys.at(i))});
        join_predicates.emplace_back(join_predicate);
    }

    join_predicates.emplace_back(step.getFilter()->clone());

    if (join_predicates.size() == 1)
    {
        return join_predicates[0];
    }
    else
    {
        return makeASTFunction(PredicateConst::AND, join_predicates);
    }
}

bool PredicateUtils::isJoinClause(
    ConstASTPtr expression, std::set<String> & left_symbols, std::set<String> & right_symbols, ContextMutablePtr & context)
{
    // At this point in time, our join predicates need to be deterministic
    if (expression->as<const ASTFunction>() && ExpressionDeterminism::isDeterministic(expression, context))
    {
        const auto & fun = expression->as<const ASTFunction &>();
        if (fun.name == "equals")
        {
            std::set<String> symbols1 = SymbolsExtractor::extract(fun.arguments->getChildren()[0]);
            std::set<String> symbols2 = SymbolsExtractor::extract(fun.arguments->getChildren()[1]);
            if (symbols1.empty() || symbols2.empty())
            {
                return false;
            }
            return (SymbolUtils::containsAll(left_symbols, symbols1) && SymbolUtils::containsAll(right_symbols, symbols2))
                || (SymbolUtils::containsAll(right_symbols, symbols1) && SymbolUtils::containsAll(left_symbols, symbols2));
        }
    }
    return false;
}

bool PredicateUtils::isJoinClauseUnmodified(
    std::set<std::pair<String, String>> & join_clauses, const Names & left_keys, const Names & right_keys)
{
    // if new join clauses size diff with origin join clauses. return false.
    if (join_clauses.size() != left_keys.size())
    {
        return false;
    }

    Names new_left_keys;
    Names new_right_keys;
    for (const auto & join_clause : join_clauses)
    {
        new_left_keys.emplace_back(join_clause.first);
        new_right_keys.emplace_back(join_clause.second);
    }
    return new_left_keys == left_keys && new_right_keys == right_keys;
}

template <typename T, enable_if_ast<T>>
void PredicateUtils::extractPredicate(const T & predicate, const std::string & fun_name, std::vector<T> & result)
{
    if (predicate && predicate->template as<const ASTFunction>())
    {
        const auto & fun = predicate->template as<const ASTFunction &>();
        if (fun.name == fun_name)
        {
            ASTs & arguments = fun.arguments->children;
            for (const auto & argument : arguments)
            {
                extractPredicate<T>(argument, fun_name, result);
            }
        }
        else
        {
            result.emplace_back(predicate);
        }
    }
    else
    {
        result.emplace_back(predicate);
    }
}

String PredicateUtils::flip(const String & fun_name)
{
    if (fun_name == PredicateConst::AND)
    {
        return PredicateConst::OR;
    }
    if (fun_name == PredicateConst::OR)
    {
        return PredicateConst::AND;
    }
    throw Exception("Unsupported function type : " + fun_name, ErrorCodes::LOGICAL_ERROR);
}

std::vector<std::pair<ConstASTPtr, String>>
PredicateUtils::removeAll(std::vector<std::pair<ConstASTPtr, String>> & collection, std::set<String> & elements_to_remove)
{
    std::vector<std::pair<ConstASTPtr, String>> keep;
    for (auto & predicate : collection)
    {
        if (!elements_to_remove.contains(predicate.second))
        {
            keep.emplace_back(predicate);
        }
    }
    return keep;
}

void CartesianRecurse(
    std::vector<std::vector<ConstASTPtr>> & accum, std::vector<ConstASTPtr> & stack,
    std::vector<std::vector<ConstASTPtr>> & sequences, int index)
{
    std::vector<ConstASTPtr> sequence = sequences[index];
    for (const auto & seq : sequence)
    {
        stack.emplace_back(seq);
        if (index == 0) {
            accum.emplace_back(std::vector<ConstASTPtr>{stack.rbegin(), stack.rend()});
        }
        else
            CartesianRecurse(accum, stack, sequences, index - 1);
        stack.pop_back();
    }
}

std::vector<std::vector<ConstASTPtr>> CartesianProduct(std::vector<std::vector<ConstASTPtr>> & sequences)
{
    std::vector<std::vector<ConstASTPtr>> accum;
    std::vector<ConstASTPtr> stack;
    if (!sequences.empty())
        CartesianRecurse(accum, stack, sequences, sequences.size() - 1);
    return accum;
}

std::vector<std::vector<ConstASTPtr>> PredicateUtils::cartesianProduct(std::vector<std::vector<ConstASTPtr>> & sets)
{
    return CartesianProduct(sets);
}

static ConstASTPtr splitDisjuncts(const ConstASTPtr & expression, const ConstASTPtr & target)
{
    auto targets = PredicateUtils::extractDisjuncts(target);
    EqualityASTSet targets_set;
    for(auto& x: targets)
    {
        targets_set.emplace(x);
    }


    auto disjuncts = PredicateUtils::extractDisjuncts(expression);
    bool size_equlas = targets.size() == disjuncts.size();
    std::erase_if(disjuncts, [&](const ConstASTPtr & disjunct) { return targets_set.count(disjunct); });
    if (!disjuncts.empty())
        return nullptr;
    else if (size_equlas)
        return PredicateConst::TRUE_VALUE;
    else
        return expression;
}

static ASTPtr splitConjuncts(const ConstASTPtr & expression, const ConstASTPtr & target)
{
    auto targets = PredicateUtils::extractConjuncts(target);
    EqualityASTSet targets_set;
    for (auto & expr : targets)
        if (!PredicateUtils::isTruePredicate(expr))
            targets_set.emplace(expr);

    auto conjuncts = PredicateUtils::extractConjuncts(expression);
    EqualityASTSet conjuncts_set;
    for (auto & expr : conjuncts)
        if (!PredicateUtils::isTruePredicate(expr))
            conjuncts_set.emplace(expr);
    bool all_contains = std::all_of(targets.begin(), targets.end(), [&](const ConstASTPtr & target_) {
        return conjuncts_set.count(target_);
    });
    if (!all_contains)
        return nullptr;

    std::erase_if(conjuncts, [&](const ConstASTPtr & conjunct) { return targets_set.count(conjunct); });
    return PredicateUtils::combineConjuncts(conjuncts);
}

ASTPtr PredicateUtils::splitPredicates(const ConstASTPtr & expression, const ConstASTPtr & target)
{
    if (PredicateUtils::isTruePredicate(target))
        return expression->clone();
    auto res = splitDisjuncts(expression, target);
    if (res)
        return res->clone();
    return splitConjuncts(expression, target);
}

std::pair<std::vector<std::pair<ConstASTPtr, ConstASTPtr>>, std::vector<ConstASTPtr>>
PredicateUtils::extractEqualPredicates(const std::vector<ConstASTPtr> & predicates)
{
    std::vector<std::pair<ConstASTPtr, ConstASTPtr>> equal_predicates;
    std::vector<ConstASTPtr> other_predicates;

    for (const auto & predicate : predicates)
    {
        for (auto & filter : PredicateUtils::extractConjuncts(predicate))
        {
            const auto * function = filter->as<ASTFunction>();
            if (function && function->name == "equals")
            {
                if (function->arguments->children.size() == 2 && function->arguments->children[0]->getType() == ASTType::ASTIdentifier
                    && function->arguments->children[1]->getType() == ASTType::ASTIdentifier)
                {
                    equal_predicates.emplace_back(function->arguments->children[0], function->arguments->children[1]);
                    continue;
                }
            }
            other_predicates.push_back(filter);
        }
    }
    return {equal_predicates, other_predicates};
}

template ASTPtr PredicateUtils::combineConjuncts<true, ASTPtr>(const std::vector<ASTPtr> & predicates);
template ASTPtr PredicateUtils::combineConjuncts<false, ASTPtr>(const std::vector<ASTPtr> & predicates);
template ASTPtr PredicateUtils::combineConjuncts<true, ConstASTPtr>(const std::vector<ConstASTPtr> & predicates);
template ASTPtr PredicateUtils::combineConjuncts<false, ConstASTPtr>(const std::vector<ConstASTPtr> & predicates);
template ASTPtr PredicateUtils::combineDisjuncts<true, ASTPtr>(const std::vector<ASTPtr> & predicates);
template ASTPtr PredicateUtils::combineDisjuncts<false, ASTPtr>(const std::vector<ASTPtr> & predicates);
template ASTPtr PredicateUtils::combineDisjuncts<true, ConstASTPtr>(const std::vector<ConstASTPtr> & predicates);
template ASTPtr PredicateUtils::combineDisjuncts<false, ConstASTPtr>(const std::vector<ConstASTPtr> & predicates);
template ASTPtr
PredicateUtils::combineDisjunctsWithDefault<true, ASTPtr>(const std::vector<ASTPtr> & predicates, const ASTPtr & default_ast);
template ASTPtr
PredicateUtils::combineDisjunctsWithDefault<false, ASTPtr>(const std::vector<ASTPtr> & predicates, const ASTPtr & default_ast);
template ASTPtr
PredicateUtils::combineDisjunctsWithDefault<true, ConstASTPtr>(const std::vector<ConstASTPtr> & predicates, const ASTPtr & default_ast);
template ASTPtr
PredicateUtils::combineDisjunctsWithDefault<false, ConstASTPtr>(const std::vector<ConstASTPtr> & predicates, const ASTPtr & default_ast);
template ASTPtr PredicateUtils::combinePredicates<true, ASTPtr>(const String & fun, std::vector<ASTPtr> predicates);
template ASTPtr PredicateUtils::combinePredicates<false, ASTPtr>(const String & fun, std::vector<ASTPtr> predicates);
template ASTPtr PredicateUtils::combinePredicates<true, ConstASTPtr>(const String & fun, std::vector<ConstASTPtr> predicates);
template ASTPtr PredicateUtils::combinePredicates<false, ConstASTPtr>(const String & fun, std::vector<ConstASTPtr> predicates);
template bool PredicateUtils::isTruePredicate<ASTPtr>(const ASTPtr & predicate);
template bool PredicateUtils::isTruePredicate<ConstASTPtr>(const ConstASTPtr & predicate);
template bool PredicateUtils::isFalsePredicate<ASTPtr>(const ASTPtr & predicate);
template bool PredicateUtils::isFalsePredicate<ConstASTPtr>(const ConstASTPtr & predicate);
template std::vector<ASTPtr> PredicateUtils::extractConjuncts<ASTPtr>(ASTPtr predicate);
template std::vector<ConstASTPtr> PredicateUtils::extractConjuncts<ConstASTPtr>(ConstASTPtr predicate);
template std::vector<ASTPtr> PredicateUtils::extractDisjuncts<ASTPtr>(ASTPtr predicate);
template std::vector<ConstASTPtr> PredicateUtils::extractDisjuncts<ConstASTPtr>(ConstASTPtr predicate);
template std::vector<ASTPtr> PredicateUtils::extractPredicate<ASTPtr>(ASTPtr predicate);
template std::vector<ConstASTPtr> PredicateUtils::extractPredicate<ConstASTPtr>(ConstASTPtr predicate);
template void
PredicateUtils::extractPredicate<ASTPtr>(const ASTPtr & predicate, const std::string & fun_name, std::vector<ASTPtr> & result);
template void PredicateUtils::extractPredicate<ConstASTPtr>(
    const ConstASTPtr & predicate, const std::string & fun_name, std::vector<ConstASTPtr> & result);
}
