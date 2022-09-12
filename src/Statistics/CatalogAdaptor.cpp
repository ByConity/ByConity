#include <Interpreters/Context.h>
#include <Statistics/CatalogAdaptor.h>
namespace DB::Statistics
{
CatalogAdaptorPtr createCatalogAdaptorMemory(ContextPtr context);
CatalogAdaptorPtr createCatalogAdaptorCnch(ContextPtr context);

CatalogAdaptorPtr createCatalogAdaptor(ContextPtr context)
{
    if (context->getSettingsRef().enable_memory_catalog)
    {
        return createCatalogAdaptorMemory(context);
    }
    return createCatalogAdaptorCnch(context);
}



}
