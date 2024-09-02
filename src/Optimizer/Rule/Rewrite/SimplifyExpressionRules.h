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

#pragma once
#include <Optimizer/Rule/Rule.h>
#include <Optimizer/DomainTranslator.h>

namespace DB {

class CommonPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::COMMON_PREDICATE_REWRITE; }
    String getName() const override { return "COMMON_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_common_predicate_rewrite; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class CommonJoinFilterRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::COMMON_JOIN_FILTER_REWRITE; }
    String getName() const override { return "COMMON_JOIN_FILTER_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_common_join_predicate_rewrite; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SwapPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SWAP_PREDICATE_REWRITE; }
    String getName() const override { return "SWAP_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_swap_predicate_rewrite; }    
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_PREDICATE_REWRITE; }
    String getName() const override { return "SIMPLIFY_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_simplify_predicate_rewrite; }    
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


class UnWarpCastInPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::UN_WARP_CAST_IN_PREDICATE_REWRITE; }
    String getName() const override { return "UN_WARP_CAST_IN_PREDICATE_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_unwrap_cast_in; }        
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyJoinFilterRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_JOIN_FILTER_REWRITE; }
    String getName() const override { return "SIMPLIFY_JOIN_FILTER_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_simplify_join_filter_rewrite; }            
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }

    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyExpressionRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_EXPRESSION_REWRITE; }
    String getName() const override { return "SIMPLIFY_EXPRESSION_REWRITE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_simplify_expression_rewrite; }            
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MergePredicatesUsingDomainTranslator : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_PREDICATES_USING_DOMAIN_TRANSLATOR; }
    String getName() const override { return "MERGE_PREDICATES_USING_DOMAIN_TRANSLATOR"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().rewrite_predicate_by_domain; }        
    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }

    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}

