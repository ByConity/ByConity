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

#include <stack>
#include <QueryPlan/QueryPlan.h>

#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Processors/QueryPipeline.h>
#include <Protos/EnumMacros.h>
#include <Protos/plan_node.pb.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/Optimizations/Optimizations.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/TableScanStep.h>
#include <google/protobuf/util/json_util.h>
#include <Common/JSONBuilder.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ENUM_WITH_PROTO_CONVERTER(
    QueryPlanMode, // enum name
    Protos::QueryPlan::PlanMode, // protobuf enum message
    (TreeLike, 1),
    (Flatten, 2));

QueryPlan::QueryPlan() = default;
QueryPlan::~QueryPlan() = default;
QueryPlan::QueryPlan(QueryPlan &&) = default;
QueryPlan & QueryPlan::operator=(QueryPlan &&) = default;

QueryPlan::QueryPlan(PlanNodePtr root_, PlanNodeIdAllocatorPtr id_allocator_)
    : plan_node(std::move(root_)), id_allocator(std::move(id_allocator_))
{
}

QueryPlan::QueryPlan(PlanNodePtr root_, CTEInfo cte_info_, PlanNodeIdAllocatorPtr id_allocator_)
    : plan_node(std::move(root_)), cte_info(std::move(cte_info_)), id_allocator(std::move(id_allocator_))
{
}

void QueryPlan::checkInitialized() const
{
    if (!isInitialized())
        throw Exception("QueryPlan was not initialized", ErrorCodes::LOGICAL_ERROR);
}

void QueryPlan::checkNotCompleted() const
{
    if (isCompleted())
        throw Exception("QueryPlan was already completed", ErrorCodes::LOGICAL_ERROR);
}

bool QueryPlan::isCompleted() const
{
    return isInitialized() && !root->step->hasOutputStream();
}

const DataStream & QueryPlan::getCurrentDataStream() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputStream();
}

void QueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<QueryPlan>> plans)
{
    if (isInitialized())
        throw Exception("Cannot unite plans because current QueryPlan is already initialized", ErrorCodes::LOGICAL_ERROR);

    const auto & inputs = step->getInputStreams();
    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != plans.size())
    {
        throw Exception(
            "Cannot unite QueryPlans using " + step->getName()
                + " because step has different number of inputs. "
                  "Has "
                + std::to_string(plans.size())
                + " plans "
                  "and "
                + std::to_string(num_inputs) + " inputs",
            ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;
        const auto & plan_header = plans[i]->getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception(
                "Cannot unite QueryPlans using " + step->getName()
                    + " because "
                      "it has incompatible header with plan "
                    + root->step->getName()
                    + " "
                      "plan header: "
                    + plan_header.dumpStructure() + "step header: " + step_header.dumpStructure(),
                ErrorCodes::LOGICAL_ERROR);
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), std::move(plan->nodes));

    nodes.emplace_back(Node{.step = std::move(step)});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan->root);

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        interpreter_context.insert(interpreter_context.end(), plan->interpreter_context.begin(), plan->interpreter_context.end());
    }
}

void QueryPlan::addStep(QueryPlanStepPtr step, PlanNodes children)
{
    (void)children;
    checkNotCompleted();

    size_t num_input_streams = step->getInputStreams().size();
    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception(
                "Cannot add step " + step->getName()
                    + " to QueryPlan because "
                      "step has no inputs, but QueryPlan is already initialized",
                ErrorCodes::LOGICAL_ERROR);

        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
        return;
    }
    else
    {
        if (!isInitialized())
            throw Exception(
                "Cannot add step " + step->getName()
                    + " to QueryPlan because "
                      "step has input, but QueryPlan is not initialized",
                ErrorCodes::LOGICAL_ERROR);

        const auto & root_header = root->step->getOutputStream().header;
        const auto & step_header = step->getInputStreams().front().header;
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception(
                "Cannot add step " + step->getName()
                    + " to QueryPlan because "
                      "it has incompatible header with root step "
                    + root->step->getName()
                    + " "
                      "root header: "
                    + root_header.dumpStructure() + "step header: " + step_header.dumpStructure(),
                ErrorCodes::LOGICAL_ERROR);

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
        return;
    }
}

void QueryPlan::addNode(Node && node_)
{
    nodes.emplace_back(std::move(node_));
}

void QueryPlan::addRoot(Node && node_)
{
    nodes.emplace_back(std::move(node_));
    root = &nodes.back();
}

/**
 * Remove nodes that have no step and children.
 * Refresh children of each node since this childen maybe removed.
 */
void QueryPlan::freshPlan()
{
    for (auto it = nodes.begin(); it != nodes.end();)
    {
        if (!it->step && it->children.empty())
            it = nodes.erase(it);
        else
            ++it;
    }

    std::unordered_set<Node *> exists_nodes;

    for (auto & node : nodes)
        exists_nodes.insert(&node);

    for (auto & node : nodes)
    {
        std::vector<Node *> freshed_children;
        for (auto & child : node.children)
        {
            if (exists_nodes.count(child))
                freshed_children.push_back(child);
        }
        node.children.swap(freshed_children);
    }
}

/**
 * Be careful, after we create a sub_plan, some nodes in the original plan have been deleted and deconstructed.
 * More precisely， nodes that moved to sub_plan are deleted.
 */
QueryPlan QueryPlan::getSubPlan(QueryPlan::Node * node_)
{
    QueryPlan sub_plan;

    std::stack<QueryPlan::Node *> plan_nodes;
    sub_plan.addRoot(Node{.step = node_->step, .children = node_->children, .id = node_->id});
    plan_nodes.push(sub_plan.getRoot());
    sub_plan.setResetStepId(reset_step_id);

    while (!plan_nodes.empty())
    {
        auto current = plan_nodes.top();
        plan_nodes.pop();

        std::vector<Node *> result_children;
        for (auto & child : current->children)
        {
            sub_plan.addNode(Node{.step = child->step, .children = child->children, .id = child->id});
            result_children.push_back(sub_plan.getLastNode());
            plan_nodes.push(sub_plan.getLastNode());
        }
        current->children.swap(result_children);
    }

    freshPlan();

    return sub_plan;
}

QueryPipelinePtr QueryPlan::buildQueryPipeline(
    const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings)
{
    checkInitialized();

    if (!optimization_settings.enable_optimizer)
    {
        optimize(optimization_settings);
    }

    struct Frame
    {
        Node * node = {};
        QueryPipelines pipelines = {};
    };

    QueryPipelinePtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});
    Stopwatch watch;
    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr; //-V1048
        }

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size())
        {
            bool limit_max_threads = frame.pipelines.empty();
            try
            {
                last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines), build_pipeline_settings);
                updatePipelineStepInfo(last_pipeline, frame.node->step, frame.node->id);
            }
            catch (const Exception & e) /// Typical for an incorrect username, password, or address.
            {
                LOG_ERROR(log, "Build pipeline error {}", e.what());
                throw;
            }
            // #ifndef NDEBUG
            //             if (optimization_settings.enable_optimizer)
            //             {
            //                 const auto & output_header = frame.node->step->getOutputStream().header;
            //                 const auto & pipeline_header = last_pipeline->getHeader();
            //                 assertBlocksHaveEqualStructure(
            //                     output_header,
            //                     pipeline_header,
            //                     "QueryPlan::buildQueryPipeline for " + frame.node->step->getName() + " (output header, pipeline header)");
            //             }
            // #endif

            if (limit_max_threads && max_threads)
                last_pipeline->limitMaxThreads(max_threads);

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    for (auto & context : interpreter_context)
        last_pipeline->addInterpreterContext(std::move(context));

    LOG_DEBUG(log, "Build pipeline takes: {}ms", watch.elapsedMilliseconds());
    return last_pipeline;
}

void QueryPlan::updatePipelineStepInfo(QueryPipelinePtr & pipeline_ptr, QueryPlanStepPtr & step, size_t step_id)
{
    auto * source_step = dynamic_cast<ISourceStep *>(step.get());
    for (const auto & processor : pipeline_ptr->getProcessors())
        if (processor->getStepId() == -1 || source_step)
            processor->setStepId(step_id);
}

Pipe QueryPlan::convertToPipe(
    const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings)
{
    if (!isInitialized())
        return {};

    if (isCompleted())
        throw Exception("Cannot convert completed QueryPlan to Pipe", ErrorCodes::LOGICAL_ERROR);

    return QueryPipeline::getPipe(std::move(*buildQueryPipeline(optimization_settings, build_pipeline_settings)));
}

void QueryPlan::addInterpreterContext(std::shared_ptr<Context> context)
{
    interpreter_context.emplace_back(std::move(context));
}


static void explainStep(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const QueryPlan::ExplainPlanOptions & options)
{
    map.add("Node Type", step.getName());

    if (options.description)
    {
        const auto & description = step.getStepDescription();
        if (!description.empty())
            map.add("Description", description);
    }

    if (options.header && step.hasOutputStream())
    {
        auto header_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & output_column : step.getOutputStream().header)
        {
            auto column_map = std::make_unique<JSONBuilder::JSONMap>();
            column_map->add("Name", output_column.name);
            if (output_column.type)
                column_map->add("Type", output_column.type->getName());

            header_array->add(std::move(column_map));
        }

        map.add("Header", std::move(header_array));
    }

    if (options.actions)
        step.describeActions(map);

    if (options.indexes)
        step.describeIndexes(map);
}

JSONBuilder::ItemPtr QueryPlan::explainPlan(const ExplainPlanOptions & options)
{
    checkInitialized();

    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
        std::unique_ptr<JSONBuilder::JSONMap> node_map = {};
        std::unique_ptr<JSONBuilder::JSONArray> children_array = {};
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    std::unique_ptr<JSONBuilder::JSONMap> tree;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (frame.next_child == 0)
        {
            if (!frame.node->children.empty())
                frame.children_array = std::make_unique<JSONBuilder::JSONArray>();

            frame.node_map = std::make_unique<JSONBuilder::JSONMap>();
            explainStep(*frame.node->step, *frame.node_map, options);
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
        {
            if (frame.children_array)
                frame.node_map->add("Plans", std::move(frame.children_array));

            tree.swap(frame.node_map);
            stack.pop();

            if (!stack.empty())
                stack.top().children_array->add(std::move(tree));
        }
    }

    return tree;
}

static void
explainStep(const IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings, const QueryPlan::ExplainPlanOptions & options)
{
    std::string prefix(settings.offset, ' ');
    settings.out << prefix;
    settings.out << step.getName();

    const auto & description = step.getStepDescription();
    if (options.description && !description.empty())
        settings.out << " (" << description << ')';

    settings.out.write('\n');

    if (options.header)
    {
        settings.out << prefix;

        if (!step.hasOutputStream())
            settings.out << "No header";
        else if (!step.getOutputStream().header)
            settings.out << "Empty header";
        else
        {
            settings.out << "Header: ";
            bool first = true;

            for (const auto & elem : step.getOutputStream().header)
            {
                if (!first)
                    settings.out << "\n" << prefix << "        ";

                first = false;
                elem.dumpNameAndType(settings.out);
            }
        }

        settings.out.write('\n');
    }

    if (options.actions)
        step.describeActions(settings);

    if (options.indexes)
        step.describeIndexes(settings);
}

std::string debugExplainStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    IQueryPlanStep::FormatSettings settings{.out = out};
    QueryPlan::ExplainPlanOptions options{.actions = true};
    explainStep(step, settings, options);
    return out.str();
}

void QueryPlan::explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options) const
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = (stack.size() - 1) * settings.indent;
            explainStep(*frame.node->step, settings, options);
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

static void explainPipelineStep(IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << "(" << step.getName() << ")";
    if (dynamic_cast<TableScanStep *>(&step))
        settings.out << " # " << step.getStepDescription();
    settings.out << "\n";
    size_t current_offset = settings.offset;
    step.describePipeline(settings);
    if (current_offset == settings.offset)
        settings.offset += settings.indent;
}

void QueryPlan::explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options) const
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        size_t offset = 0;
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = frame.offset;
            explainPipelineStep(*frame.node->step, settings);
            frame.offset = settings.offset;
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child], frame.offset});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

void QueryPlan::optimize(const QueryPlanOptimizationSettings & optimization_settings)
{
    QueryPlanOptimizations::optimizeTree(optimization_settings, *root, nodes);
}

// TODO: deprecate when proto rpc is ready
void QueryPlan::serialize(WriteBuffer & buffer) const
{
    // serialize nodes
    writeBinary(nodes.size(), buffer);
    /**
     * we first encode the query plan node for serialize / deserialize
     */
    if (reset_step_id)
    {
        size_t id = 0;
        for (auto it = nodes.begin(); it != nodes.end(); ++it)
            it->id = id++;
    }

    for (const auto & node : nodes)
    {
        serializePlanStep(node.step, buffer);
        /**
         * serialize its children ids
         */
        writeBinary(node.children.size(), buffer);
        for (auto jt = node.children.begin(); jt != node.children.end(); ++jt)
            writeBinary((*jt)->id, buffer);

        writeBinary(node.id, buffer);
    }

    if (root)
    {
        writeBinary(true, buffer);
        writeBinary(root->id, buffer);
    }
    else
        writeBinary(false, buffer);

    // serialize plan node

    if (plan_node)
    {
        writeBinary(true, buffer);
        writeBinary(plan_node->getId(), buffer);

        PlanNodes tmp_nodes;
        std::queue<PlanNodePtr> q;
        q.push(plan_node);

        for (const auto & item : cte_info.getCTEs())
        {
            q.push(item.second);
        }

        while (!q.empty())
        {
            tmp_nodes.push_back(q.front());
            for (const auto & child : q.front()->getChildren())
            {
                q.push(child);
            }
            q.pop();
        }

        writeBinary(tmp_nodes.size(), buffer);
        for (auto & node : tmp_nodes)
        {
            writeBinary(node->getId(), buffer);
            serializePlanStep(node->getStep(), buffer);

            writeBinary(node->getChildren().size(), buffer);
            for (const auto & child : node->getChildren())
            {
                writeBinary(child->getId(), buffer);
            }
        }

        writeBinary(cte_info.size(), buffer);
        for (const auto & item : cte_info.getCTEs())
        {
            writeBinary(item.first, buffer);
            writeBinary(item.second->getId(), buffer);
        }
    }
    else
    {
        writeBinary(false, buffer);
    }
}
// handle when plan is tree-like, i.e., plan_node + cte_info
void QueryPlan::toProto(Protos::QueryPlan & proto) const
{
    if (plan_node)
    {
        proto.set_mode(QueryPlanModeConverter::toProto(QueryPlanMode::TreeLike));
        toProtoTreeLike(proto);
    }
    else if (root)
    {
        proto.set_mode(QueryPlanModeConverter::toProto(QueryPlanMode::Flatten));
        toProtoFlatten(proto);
    }
    else
    {
        throw Exception("Invalid QueryPlan", ErrorCodes::LOGICAL_ERROR);
    }
}

void QueryPlan::fromProto(const Protos::QueryPlan & proto)
{
    auto mode = QueryPlanModeConverter::fromProto(proto.mode());
    switch (mode)
    {
        case QueryPlanMode::TreeLike:
            this->fromProtoTreeLike(proto);
            break;
        case QueryPlanMode::Flatten:
            this->fromProtoFlatten(proto);
            break;
        default: {
            throw Exception("Invalid QueryPlan Proto", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

// handle when plan is flatten, i.e., root + nodes + cte_nodes
void QueryPlan::toProtoFlatten(Protos::QueryPlan & proto) const
{
    if (!root)
        throw Exception("QueryPlan::toProtoFlatten() failed", ErrorCodes::LOGICAL_ERROR);

    if (reset_step_id)
    {
        size_t id = 0;
        for (const auto & node : nodes)
            node.id = id++; // this is mutable field
    }

    for (const auto & node : nodes)
    {
        auto id = node.id;
        auto & node_proto = (*proto.mutable_plan_nodes())[id];
        node_proto.set_plan_id(id);
        serializeQueryPlanStepToProto(node.step, *node_proto.mutable_step());
        for (const auto & child : node.children)
        {
            node_proto.add_children(child->id);
        }
    }

    proto.set_root_id(root->id);
}

void QueryPlan::fromProtoFlatten(const Protos::QueryPlan & proto)
{
    std::unordered_map<size_t, Node *> id_to_node;
    const auto & id_to_node_proto = proto.plan_nodes();

    auto context = !interpreter_context.empty() ? interpreter_context.back() : nullptr;

    for (const auto & [id, node_proto] : id_to_node_proto)
    {
        if (node_proto.plan_id() != id)
            throw Exception("Invalid Proto", ErrorCodes::LOGICAL_ERROR);
        auto step = deserializeQueryPlanStepFromProto(node_proto.step(), context);
        nodes.emplace_back(Node{step, {}, id});
        id_to_node[id] = &nodes.back();
    }

    for (auto & node : nodes)
    {
        auto id = node.id;
        for (auto child_id : id_to_node_proto.at(id).children())
        {
            auto * child = id_to_node[child_id];
            node.children.emplace_back(child);
        }
    }

    auto root_id = proto.root_id();
    root = id_to_node[root_id];
}

// support optimizer mode
void QueryPlan::toProtoTreeLike(Protos::QueryPlan & proto) const
{
    if (!plan_node)
        throw Exception("QueryPlan::toProtoTreeLike() failed", ErrorCodes::LOGICAL_ERROR);

    std::queue<PlanNodePtr> queue;
    queue.push(plan_node);

    proto.set_root_id(plan_node->getId());
    for (const auto & [cte_id, ptr] : this->cte_info.getCTEs())
    {
        queue.push(ptr);
        (*proto.mutable_cte_id_mapping())[cte_id] = ptr->getId();
    }

    while (!queue.empty())
    {
        auto cur = queue.front();

        auto plan_id = cur->getId();
        auto * cur_pb = &(*proto.mutable_plan_nodes())[plan_id];

        cur_pb->set_plan_id(plan_id);
        serializeQueryPlanStepToProto(cur->getStep(), *cur_pb->mutable_step());
        for (const auto & child : cur->getChildren())
        {
            queue.push(child);
            cur_pb->add_children(child->getId());
        }

        queue.pop();
    }
}

void QueryPlan::fromProtoTreeLike(const Protos::QueryPlan & proto)
{
    std::unordered_map<Int64, PlanNodePtr> id_to_plan;
    ContextPtr context = interpreter_context.empty() ? nullptr : interpreter_context.back();
    for (const auto & [plan_id, plan_pb] : proto.plan_nodes())
    {
        if (plan_pb.plan_id() != plan_id)
            throw Exception("Invalid Proto", ErrorCodes::LOGICAL_ERROR);
        auto step = deserializeQueryPlanStepFromProto(plan_pb.step(), context);
        auto plan = PlanNodeBase::createPlanNode(plan_id, step);
        id_to_plan[plan_id] = std::move(plan);
    }

    // set children
    for (const auto & [plan_id, plan_pb] : proto.plan_nodes())
    {
        PlanNodes children;
        for (auto child_id : plan_pb.children())
        {
            children.emplace_back(id_to_plan.at(child_id));
        }
        id_to_plan.at(plan_id)->replaceChildren(children);
    }

    for (auto [cte_id, plan_id] : proto.cte_id_mapping())
    {
        this->cte_info.add(cte_id, id_to_plan.at(plan_id));
    }
    auto root_id = proto.root_id();
    this->setPlanNode(id_to_plan.at(root_id));
}

// TODO: deprecate when proto rpc is ready
void QueryPlan::deserialize(ReadBuffer & buffer)
{
    size_t nodes_size;
    readBinary(nodes_size, buffer);

    std::unordered_map<size_t, Node *> map_to_node;
    std::unordered_map<size_t, std::vector<size_t>> map_to_children;

    for (size_t i = 0; i < nodes_size; ++i)
    {
        QueryPlanStepPtr step = deserializePlanStep(buffer, interpreter_context.empty() ? nullptr : interpreter_context.back());
        /**
         * deserialize children ids
         */
        size_t children_size;
        readBinary(children_size, buffer);
        std::vector<size_t> children(children_size);
        for (size_t j = 0; j < children_size; ++j)
            readBinary(children[j], buffer);

        /**
         * construct node, node-id mapping and id-children mapping
         */
        size_t id;
        readBinary(id, buffer);
        nodes.emplace_back(Node{.step = std::move(step), .id = id});
        map_to_node[id] = &nodes.back();
        map_to_children[id] = std::move(children);
    }

    bool has_root;
    readBinary(has_root, buffer);
    size_t root_id;
    if (has_root)
        readBinary(root_id, buffer);

    /**
     * After we have node-id mapping and id-children mapping,
     * fill the children infomation to each node.
     */
    for (auto & node : nodes)
    {
        size_t id = node.id;
        auto & children = map_to_children[id];
        for (auto & child_id : children)
            node.children.push_back(map_to_node[child_id]);
    }

    if (has_root)
        root = map_to_node[root_id];

    bool has_plan_node;
    readBinary(has_plan_node, buffer);
    if (has_plan_node)
    {
        PlanNodeId root_plan_id;
        readBinary(root_plan_id, buffer);

        PlanNodeId max_plan_id = root_plan_id;

        size_t node_count;
        readBinary(node_count, buffer);

        std::map<PlanNodeId, std::vector<PlanNodeId>> id_to_children;
        std::map<PlanNodeId, PlanNodePtr> id_to_node;
        for (size_t i = 0; i < node_count; ++i)
        {
            PlanNodeId id;
            readBinary(id, buffer);
            max_plan_id = std::max(max_plan_id, id);
            auto step = deserializePlanStep(buffer, interpreter_context.empty() ? nullptr : interpreter_context.back());

            PlanNodePtr plan = PlanNodeBase::createPlanNode(id, step);

            id_to_node[id] = plan;

            if (id == root_plan_id)
                plan_node = id_to_node[id];

            size_t child_size;
            readBinary(child_size, buffer);
            for (size_t child_index = 0; child_index < child_size; ++child_index)
            {
                PlanNodeId child_id;
                readBinary(child_id, buffer);
                id_to_children[id].emplace_back(child_id);
            }
        }

        for (const auto & item : id_to_children)
            for (auto child_id : item.second)
                id_to_node[item.first]->getChildren().emplace_back(id_to_node[child_id]);

        size_t cte_size;
        readBinary(cte_size, buffer);
        for (size_t i = 0; i < cte_size; i++)
        {
            CTEId cte_id;
            readBinary(cte_id, buffer);
            PlanNodeId refer_node_id;
            readBinary(refer_node_id, buffer);
            cte_info.add(cte_id, id_to_node[refer_node_id]);
        }
    }
}

std::set<StorageID> QueryPlan::allocateLocalTable(ContextPtr context)
{
    std::set<StorageID> res;
    for (const auto & node : nodes)
    {
        if (node.step->getType() == IQueryPlanStep::Type::TableScan)
        {
            auto * table_scan = dynamic_cast<TableScanStep *>(node.step.get());
            /// have to get storage_id before allocate to get original storage_id
            /// instead of storage_id in cloud
            res.insert(table_scan->getStorageID());
            table_scan->allocate(context);
        }
        else if (node.step->getType() == IQueryPlanStep::Type::TableWrite)
        {
            auto write_step = dynamic_cast<TableWriteStep *>(node.step.get());
            write_step->allocate(context);
        }
    }
    return res;
}

PlanNodePtr QueryPlan::getPlanNodeById(PlanNodeId node_id) const
{
    if (plan_node)
        if (auto res = plan_node->getNodeById(node_id))
            return res;

    for (const auto & cte : cte_info.getCTEs())
    {
        if (auto res = cte.second->getNodeById(node_id))
            return res;
    }

    return nullptr;
}

UInt32 QueryPlan::getPlanNodeCount(PlanNodePtr node)
{
    UInt32 size = 1;
    for (auto & child : node->getChildren())
        size += getPlanNodeCount(child);
    return size;
}

static PlanNodePtr copyPlanNode(const PlanNodePtr & plan, ContextMutablePtr & context)
{
    PlanNodes children;
    for (auto & child : plan->getChildren())
        children.emplace_back(copyPlanNode(child, context));
    return PlanNodeBase::createPlanNode(plan->getId(), plan->getStep()->copy(context), children, plan->getStatistics());
}

QueryPlanPtr QueryPlan::copy(ContextMutablePtr context)
{
    auto copy_plan_node = copyPlanNode(plan_node, context);
    CTEInfo copy_cte_info;
    for (const auto & [cte_id, cte_def] : cte_info.getCTEs())
        copy_cte_info.add(cte_id, copyPlanNode(cte_def, context));
    return std::make_unique<QueryPlan>(copy_plan_node, copy_cte_info, context->getPlanNodeIdAllocator());
}

void QueryPlan::prepare(const PreparedStatementContext & prepared_context)
{
    if (plan_node)
        plan_node->prepare(prepared_context);

    for (const auto & [cte_id, cte_def] : cte_info.getCTEs())
        cte_def->prepare(prepared_context);
}
}
