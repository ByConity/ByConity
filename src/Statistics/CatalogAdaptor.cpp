#include <Interpreters/Context.h>
#include <Statistics/CatalogAdaptor.h>
namespace DB::Statistics
{
CatalogAdaptorPtr createCatalogAdaptorMemory(ContextPtr context);

CatalogAdaptorPtr createCatalogAdaptor(ContextPtr context)
{
    if (context->getSettingsRef().enable_memory_catalog)
    {
        return createCatalogAdaptorMemory(context);
    }
    /// TODO: revise me
    return createCatalogAdaptorMemory(context);
}


TxnTimestamp fetchTimestamp(ContextPtr context)
{
    (void)context;

    return time(nullptr);
}

}
