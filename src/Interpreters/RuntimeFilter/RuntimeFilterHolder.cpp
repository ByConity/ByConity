#include <Interpreters/RuntimeFilter/RuntimeFilterHolder.h>

#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>

namespace DB
{
RuntimeFilterHolder::~RuntimeFilterHolder()
{
    RuntimeFilterManager::getInstance().removeRuntimeFilter(query_id, segment_id, filter_id);
}
}
