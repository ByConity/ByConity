#include <Catalog/IMultiWrite.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Catalog
{

/***
 *  Default implementation for IMultiWrite.
 */

void IMultiWrite::addPut(
    [[maybe_unused]]const String & key,
    [[maybe_unused]]const String & value,
    [[maybe_unused]]const String & expected,
    [[maybe_unused]]bool if_not_exists)
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

void IMultiWrite::addDelete([[maybe_unused]]const String & key, [[maybe_unused]]const UInt64 & expected_version)
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

bool IMultiWrite::commit([[maybe_unused]]bool allow_cas_fail)
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

bool IMultiWrite::isEmpty()
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

void IMultiWrite::setCommitTimeout([[maybe_unused]]const UInt32 & timeout_ms)
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

std::map<int, String> IMultiWrite::collectConflictInfo()
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

size_t IMultiWrite::getPutsSize()
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

size_t IMultiWrite::getDeleteSize()
{
    throw Exception("Not implemented in base class.", ErrorCodes::NOT_IMPLEMENTED);
}

IMultiWrite::~IMultiWrite() {}

}
}
