#pragma once

#include <Common/Config/ConfigProcessor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class MockGlobalContext
{
public:
    static constexpr const char * ADVISOR_SHARD = "advisor_shard";

    static MockGlobalContext & instance()
    {
        static MockGlobalContext mock_context;
        return mock_context;
    }

    ContextMutablePtr createSessionContext();

private:
    explicit MockGlobalContext();
    static XMLDocumentPtr mockConfig();

    SharedContextHolder shared_context;
    ContextMutablePtr context;
};

}
