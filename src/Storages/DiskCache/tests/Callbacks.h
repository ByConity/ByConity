#include <gmock/gmock.h>

#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>
#include <common/function_traits.h>

namespace DB::HybridCache
{
struct MockDestructor
{
    MOCK_METHOD3(call, void(HashedKey, BufferView, DestructorEvent));
};


struct MockInsertCB
{
    MOCK_METHOD2(call, void(Status, HashedKey));
};

struct MockLookupCB
{
    MOCK_METHOD3(call, void(Status, HashedKey, BufferView));
};

struct MockRemoveCB
{
    MOCK_METHOD2(call, void(Status, HashedKey));
};

template <typename MockCB>
auto toCallback(MockCB & mock)
{
    return bindThis(&MockCB::call, mock);
}

}
