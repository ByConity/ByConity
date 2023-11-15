#include <Optimizer/Dump/QueryDumper.h>

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>

using namespace DB::DumpUtils;

namespace DB
{

ContextMutablePtr QueryDumper::Query::buildQueryContextFrom(ContextPtr context) const
{
    ContextMutablePtr query_context = Context::createCopy(context);
    if (settings)
    {
        query_context->applySettingsChanges(settings->changes());
    }
    query_context->setCurrentDatabase(current_database);
    query_context->createPlanNodeIdAllocator();
    query_context->createSymbolAllocator();
    query_context->createOptimizerMetrics();
    query_context->makeQueryContext();
    return query_context;
}

Poco::JSON::Object::Ptr QueryDumper::getJsonDumpResult()
{
    Poco::JSON::Object::Ptr queries(new Poco::JSON::Object);
    for (const auto & query : query_buffer)
    {
        Poco::JSON::Object::Ptr query_object(new Poco::JSON::Object);
        query_object->set(toString(QueryInfo::query), query.query);
        query_object->set(toString(QueryInfo::current_database), query.current_database);
        query_object->set(toString(QueryInfo::frequency), query_hash_frequencies[query.recurring_hash]);
        if (query.settings)
        {
            Poco::JSON::Object::Ptr settings_change(new Poco::JSON::Object);
            query.settings->dumpToJSON(*settings_change);
            query_object->set(toString(QueryInfo::settings), settings_change);
        }
        for (const auto & info : query.info)
        {
            query_object->set(toString(info.first), info.second);
        }

        queries->set(query.query_id, query_object);
    }
    return queries;
}

}
