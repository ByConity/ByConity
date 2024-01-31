#pragma once

#include <CloudServices/ICnchBGThread.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class CnchObjectColumnSchemaAssembleThread : public ICnchBGThread
{
public:
    CnchObjectColumnSchemaAssembleThread(ContextPtr context_, const StorageID & id_);

private:
    void runImpl() override;
};
}//namespace DB end
