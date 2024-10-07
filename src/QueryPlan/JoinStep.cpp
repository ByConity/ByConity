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

#include <memory>
#include <Core/SettingsEnums.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/NestedLoopJoin.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterConsumer.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPlan/JoinStep.h>
#include <Common/ErrorCodes.h>
#include <common/logger_useful.h>
#include <Core/ColumnWithTypeAndName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinPtr JoinStep::makeJoin(
    ContextPtr context,
    std::shared_ptr<RuntimeFilterConsumer> && consumer,
    size_t num_streams,
    ExpressionActionsPtr filter_action,
    String filter_column_name)
{
    const auto & settings = context->getSettingsRef();
    auto table_join = std::make_shared<TableJoin>(settings, context->getTemporaryVolume());
    if (consumer)
        table_join->setRuntimeFilterConsumer(consumer);

    if (kind != ASTTableJoin::Kind::Inner && kind != ASTTableJoin::Kind::Cross)
        table_join->setInequalCondition(filter_action, filter_column_name);

    // todo support storage join
    //    if (table_to_join.database_and_table_name)
    //    {
    //        auto joined_table_id = context->resolveStorageID(table_to_join.database_and_table_name);
    //        StoragePtr table = DatabaseCatalog::instance().tryGetTable(joined_table_id, context);
    //        if (table)
    //        {
    //            if (dynamic_cast<StorageJoin *>(table.get()) ||
    //                dynamic_cast<StorageDictionary *>(table.get()))
    //                table_join->joined_storage = table;
    //        }
    //    }
    table_join->setColumnsFromJoinedTable(input_streams[1].header.getNamesAndTypesList());
    table_join->deduplicateAndQualifyColumnNames(input_streams[0].header.getNameSet(), "");

    auto using_ast = std::make_shared<ASTExpressionList>();
    ASTs on_ast_terms;
    for (size_t index = 0; index < left_keys.size(); ++index)
    {
        ASTPtr left = std::make_shared<ASTIdentifier>(left_keys[index]);
        ASTPtr right = std::make_shared<ASTIdentifier>(right_keys[index]);
        if (has_using)
        {
            table_join->renames[left_keys[index]] = right_keys[index];
            table_join->addUsingKey(left, settings.join_using_null_safe);
            using_ast->children.emplace_back(left);
        }
        else
        {
            bool null_safe = getKeyIdNullSafe(index);
            table_join->addOnKeys(left, right, null_safe);
            const String fn = null_safe ? "bitEquals" : "equals";
            on_ast_terms.emplace_back(makeASTFunction(fn, left, right));
        }
    }

    if (has_using)
    {
        table_join->table_join.using_expression_list = using_ast;
    }
    else
    {
        if (on_ast_terms.size() == 1)
            table_join->table_join.on_expression = on_ast_terms.back();
        else if (on_ast_terms.size() > 1)
            table_join->table_join.on_expression = makeASTFunction("and", on_ast_terms);
    }

    for (const auto & item : output_stream->header)
    {
        if (!input_streams[0].header.has(item.name))
        {
            NameAndTypePair joined_column{item.name, item.type};
            table_join->addJoinedColumn(joined_column);
        }
    }

    // add the symbol needed in the join filter but not existed in join output stream to the output,
    // because FilterTransform built after the join need these symbols.
    if (filter && !PredicateUtils::isTruePredicate(filter))
    {
        for (const auto & symbol : SymbolsExtractor::extract(filter))
        {
            if (!output_stream->header.has(symbol) && input_streams[1].header.has(symbol))
            {
                NameAndTypePair joined_column{symbol, input_streams[1].header.getByName(symbol).type};
                table_join->addJoinedColumn(joined_column);
            }
        }
    }

    table_join->setAsofInequality(asof_inequality);
    if (context->getSettingsRef().enforce_all_join_to_any_join)
    {
        strictness = ASTTableJoin::Strictness::RightAny;
    }

    table_join->table_join.strictness = isCrossJoin() ? ASTTableJoin::Strictness::Unspecified : strictness;
    table_join->table_join.kind = isCrossJoin() ? ASTTableJoin::Kind::Cross : kind;

    if (enforceNestLoopJoin())
    {
        if (!settings.enable_nested_loop_join)
            throw Exception("set enable_nested_loop_join=1 to enable outer join with filter", ErrorCodes::NOT_IMPLEMENTED);
        table_join->setJoinAlgorithm(JoinAlgorithm::NESTED_LOOP_JOIN);
        table_join->table_join.on_expression = filter->clone();
        table_join->table_join.kind = isCrossJoin() ? ASTTableJoin::Kind::Inner : kind;
    }

    bool allow_merge_join = table_join->allowMergeJoin();
    bool allow_grace_hash_join = true;
    if (context->getSettingsRef().use_grace_hash_only_repartition && distribution_type != DistributionType::REPARTITION)
        allow_grace_hash_join = false;
    /// HashJoin with Dictionary optimisation
    auto l_sample_block = input_streams[0].header;
    auto r_sample_block = input_streams[1].header;
    String dict_name;
    String key_name;
    if (table_join->forceNestedLoopJoin())
        return std::make_shared<NestedLoopJoin>(table_join, r_sample_block, context);
    else if (table_join->forceHashJoin() || (table_join->preferMergeJoin() && !allow_merge_join))
    {
        if (table_join->allowParallelHashJoin() && join_algorithm == JoinAlgorithm::PARALLEL_HASH)
        {
            // TODO: Yuanning RuntimeFilter, compare with CE code when fix
            // if (enable_parallel_hash_join)
            // {
            //     LOG_TRACE(getLogger("JoinStep::makeJoin"), "will use parallel Hash Join");
            //     std::vector<JoinPtr> res;
            //     res.reserve(num_streams);
            //     for (size_t i = 0; i < num_streams; ++i)
            //         res.emplace_back(std::make_shared<HashJoin>(table_join, r_sample_block));

            //     if (consumer)
            //         consumer->fixParallel(num_streams);
            //     return res;
            // }
            LOG_TRACE(getLogger("JoinStep::makeJoin"), "will use ConcurrentHashJoin");
            if (consumer)
                consumer->fixParallel(ConcurrentHashJoin::toPowerOfTwo(std::min<size_t>(num_streams, 256)));
            return std::make_shared<ConcurrentHashJoin>(table_join, num_streams, context->getSettings().parallel_join_rows_batch_threshold, r_sample_block);
        }
        else if (join_algorithm == JoinAlgorithm::GRACE_HASH && GraceHashJoin::isSupported(table_join) && allow_grace_hash_join)
        {
            if (GraceHashJoin::isSupported(table_join) ) {
                table_join->join_algorithm = JoinAlgorithm::GRACE_HASH;
                // todo aron let optimizer decide this(parallel)
                auto parallel = (context->getSettingsRef().grace_hash_join_left_side_parallel != 0 ? context->getSettingsRef().grace_hash_join_left_side_parallel: num_streams);
                return std::make_shared<GraceHashJoin>(context, table_join, l_sample_block, r_sample_block, context->getTempDataOnDisk(), parallel, context->getSettingsRef().spill_mode == SpillMode::AUTO, false, num_streams);
            } else if (allow_merge_join) { // fallback into merge join
                LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join is not support, fallback into merge join.");
                return std::make_shared<JoinSwitcher>(table_join, r_sample_block);
            } else { // fallback into hash join when grace hash and merge join not supported
                LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join and merge join is not support, fallback into hash join.");
                return std::make_shared<HashJoin>(table_join, r_sample_block);
            }
        }
        return std::make_shared<HashJoin>(table_join, r_sample_block);
    }
    else if (table_join->forceMergeJoin() || (table_join->preferMergeJoin() && allow_merge_join))
        return {std::make_shared<MergeJoin>(table_join, r_sample_block)};
    else if ((table_join->forceGraceHashJoin() || join_algorithm == JoinAlgorithm::GRACE_HASH) && allow_grace_hash_join)
    {
        if (GraceHashJoin::isSupported(table_join) ) {
            auto parallel = (context->getSettingsRef().grace_hash_join_left_side_parallel != 0 ? context->getSettingsRef().grace_hash_join_left_side_parallel: num_streams);
            return std::make_shared<GraceHashJoin>(context, table_join, l_sample_block, r_sample_block, context->getTempDataOnDisk(), parallel, context->getSettingsRef().spill_mode == SpillMode::AUTO, false, num_streams);
        } else if (allow_merge_join) { // fallback into merge join
            LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join is not support, fallback into merge join.");
            return std::make_shared<JoinSwitcher>(table_join, r_sample_block);
        } else { // fallback into hash join when grace hash and merge join not supported
            LOG_WARNING(getLogger("JoinStep::makeJoin"), "Grace hash join and merge join is not support, fallback into hash join.");
            return std::make_shared<HashJoin>(table_join, r_sample_block);
        }
    }
    return std::make_shared<JoinSwitcher>(table_join, r_sample_block);
}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_,
    bool is_ordered_,
    bool simple_reordered_,
    PlanHints hints_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , max_streams(max_streams_)
    , keep_left_read_in_order(keep_left_read_in_order_)
    , is_ordered(is_ordered_)
    , simple_reordered(simple_reordered_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream{
        .header = JoiningTransform::transformHeader(left_stream_.header, join),
    };
    hints = std::move(hints_);
}

JoinStep::JoinStep(
    DataStreams input_streams_,
    DataStream output_stream_,
    ASTTableJoin::Kind kind_,
    ASTTableJoin::Strictness strictness_,
    size_t max_streams_,
    bool keep_left_read_in_order_,
    Names left_keys_,
    Names right_keys_,
    std::vector<bool> key_ids_null_safe_,
    ConstASTPtr filter_,
    bool has_using_,
    std::optional<std::vector<bool>> require_right_keys_,
    ASOF::Inequality asof_inequality_,
    DistributionType distribution_type_,
    JoinAlgorithm join_algorithm_,
    bool is_magic_,
    bool is_ordered_,
    bool simple_reordered_,
    LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filter_builders_,
    PlanHints hints_)
    : kind(kind_)
    , strictness(strictness_)
    , max_streams(max_streams_)
    , keep_left_read_in_order(keep_left_read_in_order_)
    , left_keys(std::move(left_keys_))
    , right_keys(std::move(right_keys_))
    , key_ids_null_safe(std::move(key_ids_null_safe_))
    , filter(std::move(filter_))
    , has_using(has_using_)
    , require_right_keys(std::move(require_right_keys_))
    , asof_inequality(asof_inequality_)
    , distribution_type(distribution_type_)
    , join_algorithm(join_algorithm_)
    , is_magic(is_magic_)
    , is_ordered(is_ordered_)
    , simple_reordered(simple_reordered_)
    , runtime_filter_builders(std::move(runtime_filter_builders_))
{
    assert(!isComma(kind));
    assert(left_keys.size() == right_keys.size());
    // fixme@kaixi
    // assert(!isCross(kind) || isUnspecified(strictness)); // CROSS JOIN must use Unspecified strictness

    input_streams = std::move(input_streams_);
    output_stream = std::move(output_stream_);
    hints = std::move(hints_);
}

bool JoinStep::hasKeyIdNullSafe() const
{
    return std::any_of(key_ids_null_safe.begin(), key_ids_null_safe.end(), [](auto x) { return x; });
}

bool JoinStep::getKeyIdNullSafe(size_t key_index) const
{
    if (key_index >= key_ids_null_safe.size())
        return false;
    return key_ids_null_safe.at(key_index);
}

void JoinStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
}

void JoinStep::setOutputStream(DataStream output_stream_)
{
    output_stream = std::move(output_stream_);
}

QueryPipelinePtr JoinStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    bool need_build_runtime_filter = false;

    ExpressionActionsPtr filter_action;
    if (!join)
    {
        if (filter && !PredicateUtils::isTruePredicate(filter))
        {
            Names output;

            bool has_outer_join_semantic = settings.context->getSettingsRef().join_use_nulls && 
                (isAny(getStrictness()) || isAll(getStrictness()) || getStrictness() == ASTTableJoin::Strictness::RightAny || isAsof(getStrictness()));
            bool make_nullable_for_left = has_outer_join_semantic && isRightOrFull(getKind());
            bool make_nullable_for_right = has_outer_join_semantic && isLeftOrFull(getKind());

            Block header;
            for (const auto & col : input_streams[0].header)
            {
                if (make_nullable_for_left && JoinCommon::canBecomeNullable(col.type))
                {
                    header.insert(ColumnWithTypeAndName{col.column, JoinCommon::convertTypeToNullable(col.type), col.name});
                }
                else
                {
                    header.insert(col);
                }
            }
            for (const auto & col : input_streams[1].header)
            {
                if (make_nullable_for_right && JoinCommon::canBecomeNullable(col.type))
                {
                    header.insert(ColumnWithTypeAndName{col.column, JoinCommon::convertTypeToNullable(col.type), col.name});
                }
                else
                {
                    header.insert(col);
                }
            }
            for (const auto & item : header)
                output.emplace_back(item.name);
            output.emplace_back(filter->getColumnName());

            auto actions_dag = createExpressionActions(settings.context, header.getNamesAndTypesList(), output, filter->clone());
            filter_action = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());
        }

        if (!runtime_filter_builders.empty() && settings.distributed_settings.is_distributed)
        {
            auto builder = createRuntimeFilterBuilder(settings.context);
            std::shared_ptr<RuntimeFilterConsumer> consumer = std::make_shared<RuntimeFilterConsumer>(
                builder,
                settings.context->getInitialQueryId(),
                1, /// for normal HashJoin only one right table, parallel or concurrent hash join will change it to num_streams
                settings.distributed_settings.parallel_size,
                settings.distributed_settings.coordinator_address,
                settings.context->getPlanSegmentInstanceId().parallel_index); // TODO: Yuanning RuntimeFilter, parallel_id

            join = makeJoin(settings.context, std::move(consumer), pipelines[0]->getNumStreams(), filter_action, filter->getColumnName());
            need_build_runtime_filter = true;
        }
        else
            join = makeJoin(settings.context, nullptr, pipelines[0]->getNumStreams(), filter_action, filter->getColumnName());
        max_block_size = settings.context->getSettingsRef().max_block_size;
    }

    auto pipeline = QueryPipeline::joinPipelines(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        settings.context->getSettingsRef().join_parallel_left_right,
        &processors,
        need_build_runtime_filter);

    // if NestLoopJoin is choose, no need to add filter stream.
    if (filter && !PredicateUtils::isTruePredicate(filter) && join->getType() != JoinType::NestedLoop
        && (kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Cross))
    {
        Names output;
        auto header = pipeline->getHeader();
        for (const auto & item : header)
            output.emplace_back(item.name);
        output.emplace_back(filter->getColumnName());

        auto actions_dag = createExpressionActions(settings.context, header.getNamesAndTypesList(), output, filter->clone());
        auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

        pipeline->addSimpleTransform([&](const Block & input_header, QueryPipeline::StreamType stream_type) {
            bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
            return std::make_shared<FilterTransform>(input_header, expression, filter->getColumnName(), true, on_totals);
        });
    }

    projection(*pipeline, output_stream->header, settings);
    return pipeline;
}

bool JoinStep::enforceNestLoopJoin() const
{
    if (filter && !PredicateUtils::isTruePredicate(filter))
    {
        bool strictness_join = strictness == ASTTableJoin::Strictness::Any || strictness == ASTTableJoin::Strictness::Asof;
        return strictness_join || (left_keys.empty() && isLeftOrRightOuterJoin());
    }
    return false;
}

bool JoinStep::enforceGraceHashJoin() const
{
    return false;
}

bool JoinStep::supportReorder(bool support_filter, bool support_cross) const
{
    if (!support_filter && !PredicateUtils::isTruePredicate(filter))
        return false;

    if (require_right_keys || has_using)
        return false;

    if (hasKeyIdNullSafe())
        return false;

    if (strictness != ASTTableJoin::Strictness::Unspecified && strictness != ASTTableJoin::Strictness::All)
        return false;

    bool cross_join = isCrossJoin();
    if (!support_cross && cross_join)
        return false;

    if (support_cross && cross_join)
        return true;

    return kind == ASTTableJoin::Kind::Inner && !left_keys.empty();
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::toProto(Protos::JoinStep & proto, bool for_hash_equals) const
{
    if (for_hash_equals)
    {
        // skip
    }
    else if (output_stream.has_value())
            {
        for (const auto & element : input_streams)
            element.toProto(*proto.add_input_streams());
        output_stream->toProto(*proto.mutable_output_stream());
    }
    else
        throw Exception("required to have output stream", ErrorCodes::PROTOBUF_BAD_CAST);

    proto.set_step_description(step_description);
    proto.set_kind(ASTTableJoin::KindConverter::toProto(kind));
    proto.set_strictness(ASTTableJoin::StrictnessConverter::toProto(strictness));
    proto.set_max_streams(max_streams);
    proto.set_keep_left_read_in_order(keep_left_read_in_order);
    for (const auto & element : left_keys)
        proto.add_left_keys(element);
    for (const auto & element : right_keys)
        proto.add_right_keys(element);
    for (bool element : key_ids_null_safe)
        proto.add_key_ids_null_safe(element);
    serializeASTToProto(filter, *proto.mutable_filter());
    proto.set_has_using(has_using);
    proto.set_flag_require_right_keys(require_right_keys.has_value());
    if (require_right_keys.has_value())
        for (bool element : require_right_keys.value())
            proto.add_require_right_keys(element);
    proto.set_asof_inequality(ASOF::InequalityConverter::toProto(asof_inequality));
    proto.set_distribution_type(DistributionTypeConverter::toProto(distribution_type));
    proto.set_join_algorithm(JoinAlgorithmConverter::toProto(join_algorithm));
    proto.set_is_magic(is_magic);
    proto.set_is_ordered(is_ordered);
    for (const auto & [k, v] : runtime_filter_builders)
    {
        auto * proto_element = proto.add_runtime_filter_builders();
        proto_element->set_key(k);
        v.toProto(*proto_element->mutable_value());
    }
}

std::shared_ptr<JoinStep> JoinStep::fromProto(const Protos::JoinStep & proto, ContextPtr)
{
    DataStreams input_streams;
    for (const auto & proto_element : proto.input_streams())
    {
        DataStream element;
        element.fillFromProto(proto_element);
        input_streams.emplace_back(std::move(element));
    }
    DataStream output_stream;
    if (proto.has_output_stream())
        output_stream.fillFromProto(proto.output_stream());
    else
        throw Exception("required to have output stream", ErrorCodes::PROTOBUF_BAD_CAST);
    const auto & step_description = proto.step_description();
    auto kind = ASTTableJoin::KindConverter::fromProto(proto.kind());
    auto strictness = ASTTableJoin::StrictnessConverter::fromProto(proto.strictness());
    auto max_streams = proto.max_streams();
    auto keep_left_read_in_order = proto.keep_left_read_in_order();
    std::vector<String> left_keys;
    for (const auto & element : proto.left_keys())
        left_keys.emplace_back(element);
    std::vector<String> right_keys;
    for (const auto & element : proto.right_keys())
        right_keys.emplace_back(element);
    std::vector<bool> key_ids_null_safe;
    for (const auto & null_safe : proto.key_ids_null_safe())
        key_ids_null_safe.emplace_back(null_safe);
    auto filter = deserializeASTFromProto(proto.filter());
    auto has_using = proto.has_using();
    std::optional<std::vector<bool>> require_right_keys;
    if (proto.flag_require_right_keys())
        require_right_keys = std::vector<bool>(proto.require_right_keys().begin(), proto.require_right_keys().end());
    auto asof_inequality = ASOF::InequalityConverter::fromProto(proto.asof_inequality());
    auto distribution_type = DistributionTypeConverter::fromProto(proto.distribution_type());
    auto join_algorithm = JoinAlgorithmConverter::fromProto(proto.join_algorithm());
    auto is_magic = proto.is_magic();
    auto is_ordered = proto.is_ordered();

    LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filter_builders;
    for (const auto & element : proto.runtime_filter_builders())
    {
        auto key = element.key();
        auto value = RuntimeFilterBuildInfos::fromProto(element.value());
        runtime_filter_builders.emplace(key, value);
    }
    auto step = std::make_shared<JoinStep>(
        input_streams,
        output_stream,
        kind,
        strictness,
        max_streams,
        keep_left_read_in_order,
        left_keys,
        right_keys,
        key_ids_null_safe,
        filter,
        has_using,
        require_right_keys,
        asof_inequality,
        distribution_type,
        join_algorithm,
        is_magic,
        is_ordered,
        is_ordered,
        runtime_filter_builders);
    step->setStepDescription(step_description);
    return step;
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(input_stream_, JoiningTransform::transformHeader(input_stream_.header, join_), getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = JoiningTransform::transformHeader(input_streams_[0].header, join);
}

void FilledJoinStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(
            header, join, max_block_size, on_totals, default_totals, settings.context->getSettingsRef().join_parallel_left_right, counter);
    });
}

std::shared_ptr<IQueryPlanStep> JoinStep::copy(ContextPtr) const
{
    return std::make_shared<JoinStep>(
        input_streams,
        output_stream.value(),
        kind,
        strictness,
        max_streams,
        keep_left_read_in_order,
        left_keys,
        right_keys,
        std::move(key_ids_null_safe),
        filter,
        has_using,
        require_right_keys,
        asof_inequality,
        distribution_type,
        join_algorithm,
        is_magic,
        is_ordered,
        simple_reordered,
        runtime_filter_builders,
        hints);
}

RuntimeFilterBuilderPtr JoinStep::createRuntimeFilterBuilder(ContextPtr context) const
{
    return std::make_shared<RuntimeFilterBuilder>(context->getSettingsRef(), runtime_filter_builders);
}

bool JoinStep::mustReplicate() const
{
    if (left_keys.empty() && (kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross))
    {
        // There is nothing to partition on
        return true;
    }
    return false;
}

bool JoinStep::mustRepartition() const
{
    return kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Full;
}

std::shared_ptr<IQueryPlanStep> FilledJoinStep::copy(ContextPtr) const
{
    return std::make_shared<FilledJoinStep>(input_streams[0], join, max_block_size);
}

}
