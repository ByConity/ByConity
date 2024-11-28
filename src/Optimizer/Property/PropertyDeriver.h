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

#include <Optimizer/Property/Property.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanVisitor.h>


namespace DB
{
class PropertyDeriver
{
public:
    static Property deriveProperty(QueryPlanStepPtr step, ContextMutablePtr & context, const Property & require);
    static Property deriveProperty(PlanNodePtr node, ContextMutablePtr & context, CTEInfo & cte_info, bool ignore_null = true);
    static Property deriveProperty(QueryPlanStepPtr step, Property & input_property, const Property & require, ContextMutablePtr & context);
    static Property
    deriveProperty(QueryPlanStepPtr step, PropertySet & input_properties, const Property & require, ContextMutablePtr & context);
    static Property deriveStorageProperty(const StoragePtr & storage, const Property & require, ContextMutablePtr & context);

    static Property
    deriveStoragePropertyWhatIfMode(const StoragePtr & storage, ContextMutablePtr & context, const Property & required_property);
};

class DeriverContext
{
public:
    DeriverContext(PropertySet input_properties_, const Property & require_, ContextMutablePtr & context_, bool ignore_null_ = false)
        : input_properties(std::move(input_properties_)), require(require_), context(context_), ignore_null(ignore_null_)
    {
    }
    const PropertySet & getInput() { return input_properties; }
    ContextMutablePtr & getContext() { return context; }
    const Property & getRequire() const { return require; }
    bool isIgnoreNull() const { return ignore_null; }

private:
    PropertySet input_properties;
    const Property & require;
    ContextMutablePtr & context;
    bool ignore_null;
};

class DeriverVisitor : public StepVisitor<Property, DeriverContext>
{
public:
    Property visitStep(const IQueryPlanStep &, DeriverContext &) override;

#define VISITOR_DEF(TYPE) Property visit##TYPE##Step(const TYPE##Step & step, DeriverContext & context) override;
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

class PlanDeriverVisitor : public PlanNodeVisitor<Property, ContextMutablePtr>
{
public:
    PlanDeriverVisitor(CTEInfo & cte_info, bool ignore_null_) : cte_helper(cte_info), ignore_null(ignore_null_) { }

    Property visitPlanNode(PlanNodeBase &, ContextMutablePtr &) override;
    Property visitCTERefNode(CTERefNode & node, ContextMutablePtr & context) override;

private:
    SimpleCTEVisitHelper<Property> cte_helper;
    bool ignore_null;
};

}
