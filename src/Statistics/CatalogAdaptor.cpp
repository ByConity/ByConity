#include <ctime>
#include <Interpreters/Context.h>
#include <Statistics/CatalogAdaptor.h>

namespace DB::Statistics
{
CatalogAdaptorPtr createCatalogAdaptorMemory(ContextPtr context);
CatalogAdaptorPtr createCatalogAdaptorHa(ContextPtr context);

CatalogAdaptorPtr createCatalogAdaptor(ContextPtr context)
{
    if (context->getSettingsRef().enable_memory_catalog)
    {
        return createCatalogAdaptorMemory(context);
    }
    return createCatalogAdaptorHa(context);
}


TxnTimestamp fetchTimestamp(ContextPtr context)
{
    (void)context;

    return time(nullptr);
}

}
