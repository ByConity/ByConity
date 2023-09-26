#include <gmock/gmock.h>

#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>

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

template <typename Class, typename RetType, typename... Args>
inline std::function<RetType(Args...)> bindThis(RetType (Class::*memFn)(Args...), Class & self)
{
    return [memFn, p = &self](Args... args) { return (p->*memFn)(std::forward<Args>(args)...); };
}

template <typename MockCB>
auto toCallback(MockCB & mock)
{
    return bindThis(&MockCB::call, mock);
}

}
