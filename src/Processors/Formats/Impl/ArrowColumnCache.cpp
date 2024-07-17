#include "Processors/Formats/Impl/ArrowColumnCache.h"

namespace DB
{

std::optional<ArrowColumnCache> ArrowColumnCache::cache;

std::optional<ArrowFooterCache> ArrowFooterCache::cache;

}
