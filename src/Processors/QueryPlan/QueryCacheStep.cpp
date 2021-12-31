#include <Processors/QueryPlan/QueryCacheStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Sources//SourceFromQueryCache.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Functions/IFunction.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
        {
            {
                .preserves_distinct_columns = false,
                .returns_single_stream = true,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }
        };
}

DataStream createOutputStream(
    const DataStream & input_stream,
    Block output_header,
    const ITransformingStep::DataStreamTraits & stream_traits)
{
    DataStream output_stream{.header = std::move(output_header)};

    if (stream_traits.preserves_distinct_columns)
        output_stream.distinct_columns = input_stream.distinct_columns;

    output_stream.has_single_port = stream_traits.returns_single_stream
        || (input_stream.has_single_port && stream_traits.preserves_number_of_streams);

    if (stream_traits.preserves_sorting)
    {
        output_stream.sort_description = input_stream.sort_description;
        output_stream.sort_mode = input_stream.sort_mode;
    }

    return output_stream;
}

QueryCacheStep::QueryCacheStep(const DataStream & input_stream_,
                               const ASTPtr & query_ptr_,
                               const ContextPtr & context_,
                               QueryProcessingStage::Enum stage_)
    : query_ptr(query_ptr_), context(context_), stage(stage_)
{
    init();

    if (hitCache())
    {
        output_stream = std::move(input_stream_);
    }
    else
    {
        auto traits = getTraits();
        auto header = input_stream_.header;
        input_streams.emplace_back(std::move(input_stream_));
        output_stream = createOutputStream(input_streams.front(), std::move(header), traits.data_stream_traits);
    }
}

void QueryCacheStep::init()
{
    if (context->getSettings().enable_query_cache && isViableQuery())
    {
        query_cache = context->getQueryCache();
        query_key = std::make_shared<QueryKey>(queryToString(query_ptr), context->getSettings(), stage);
        if (query_cache)
        {
            UInt128 key = QueryCache::hash(*query_key);
            query_result = query_cache->get(key);
            if (!query_result)
            {
                query_result = std::make_shared<QueryResult>();
                ProfileEvents::increment(ProfileEvents::QueryCacheMisses);
            }
            else
            {
                hit_query_cache = true;
                ProfileEvents::increment(ProfileEvents::QueryCacheHits);
            }
        }
    }

    can_drop_cache = canDropQueryCache();
}

QueryPipelinePtr QueryCacheStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings)
{
    // if hit query cache, generate pipeline
    if (hitCache())
    {
        auto pipeline = std::make_unique<QueryPipeline>();
        QueryPipelineProcessorsCollector collector(*pipeline, this);

        initializePipeline(*pipeline, settings);

        auto added_processors = collector.detachProcessors();
        processors.insert(processors.end(), added_processors.begin(), added_processors.end());
        return pipeline;
    }

    // else, update pipeline
    QueryPipelineProcessorsCollector collector(*pipelines.front(), this);
    transformPipeline(*pipelines.front(), settings);
    processors = collector.detachProcessors();

    return std::move(pipelines.front());
}

void QueryCacheStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
                                {
                                    if (stream_type != QueryPipeline::StreamType::Main)
                                        return nullptr;

                                    return std::make_shared<QueryCacheTransform>(header, query_cache, query_key, query_result, ref_db_and_table);
                                });
}

void QueryCacheStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<SourceFromQueryCache>(getOutputStream().header, query_cache, query_key, query_result)));
}

void QueryCacheStep::checkDeterministic(const ASTPtr & node)
{
    if (!is_deterministic || !node)
        return;

    if (auto * function = node->as<ASTFunction>())
    {
        if (AggregateFunctionFactory::instance().hasNameOrAlias(function->name))
            return;

        const auto func = FunctionFactory::instance().get(function->name, context);
        if (!func->isDeterministic())
        {
            is_deterministic = false;
            return;
        }
    }

    for (const auto & child : node->children)
        checkDeterministic(child);
}

bool QueryCacheStep::isViableQuery()
{
    if (!query_ptr)
        return false;

    const ASTSelectWithUnionQuery * query = typeid_cast<ASTSelectWithUnionQuery *>(query_ptr.get());

    if (!query)
        return false;

    // Get query in lower case
    String low_query = Poco::toLower(queryToString(query_ptr));
    size_t pos = 0;
    parseSlowQuery(low_query, pos);
    if (pos != std::string::npos)
        low_query = low_query.substr(pos);

    if (startsWith(low_query, "select") && low_query.find("system.") != std::string::npos)
        return false;

    std::vector<ASTPtr> all_base_tables;
    bool dummy = false;
    query->collectAllTables(all_base_tables, dummy);

    // If there is no target table, do not use query cache
    if (all_base_tables.empty())
        return false;

    for (auto & table : all_base_tables)
    {
        auto target_table_id = context->resolveStorageID(table);
        auto storage_table = DatabaseCatalog::instance().tryGetTable(target_table_id, context);

        if (!storage_table)
            continue;

        auto * mater_tree_data = dynamic_cast<MergeTreeData *>(storage_table.get());
        if (!mater_tree_data)
            return false;

        String database = target_table_id.database_name;
        if (database.empty())
            database = context->getCurrentDatabase();

        // Add db and table into ref_db_and_table from which we can drop specific queries
        updateRefDatabaseAndTable(database, target_table_id.table_name);
    }

    checkDeterministic(query_ptr);
    return is_deterministic;
}

template <typename T>
bool QueryCacheStep::analyzeQuery()
{
    if (!query_ptr)
        return false;

    const T * query = typeid_cast<T *>(query_ptr.get());
    if (!query)
        return false;

    String database = query->database;
    if (database.empty())
        database = context->getCurrentDatabase();
    updateRefDatabaseAndTable(database, query->table);
    return true;
}

template <>
bool QueryCacheStep::analyzeQuery<ASTInsertQuery>()
{
    if (!query_ptr)
        return false;

    const ASTInsertQuery * query = typeid_cast<ASTInsertQuery *>(query_ptr.get());
    if (!query)
        return false;

    String database = query->table_id.database_name;
    if (database.empty())
        database = context->getCurrentDatabase();
    updateRefDatabaseAndTable(database, query->table_id.table_name);
    return true;
}

bool QueryCacheStep::canDropQueryCache()
{
    if (!query_ptr)
        return false;

    if (analyzeQuery<ASTInsertQuery>()
        || analyzeQuery<ASTAlterQuery>()
        || analyzeQuery<ASTDropQuery>()
        || analyzeQuery<ASTCreateQuery>()
        || analyzeQuery<ASTOptimizeQuery>())
        return true;

    const ASTRenameQuery * rename_query = typeid_cast<ASTRenameQuery *>(query_ptr.get());

    if (!rename_query)
        return false;

    for (const auto & elem : rename_query->elements)
    {
        String from_database = elem.from.database;
        if (from_database.empty())
            from_database = context->getCurrentDatabase();
        updateRefDatabaseAndTable(from_database, elem.from.table);

        String to_database = elem.to.database;
        if (to_database.empty())
            to_database = context->getCurrentDatabase();
        updateRefDatabaseAndTable(to_database, elem.to.table);
    }

    return true;
}

void QueryCacheStep::dropCache()
{
    if (!can_drop_cache)
        return;

    for (const auto & name : ref_db_and_table)
        context->dropQueryCache(name);
}

void QueryCacheStep::serialize(WriteBuffer &) const
{
    throw Exception("QueryCacheStep can not be serialized", ErrorCodes::LOGICAL_ERROR);
}

QueryPlanStepPtr QueryCacheStep::deserialize(ReadBuffer &, ContextPtr )
{
    return nullptr;
}

}
