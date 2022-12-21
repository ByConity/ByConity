#pragma once

#include <DaemonManager/DaemonJob.h>
#include <Catalog/Catalog.h>

namespace DB::Protos
{
class DataModelTable;
}

namespace DB::DaemonManager
{

class DaemonJobGlobalGC : public DaemonJob
{
public:
    DaemonJobGlobalGC(ContextMutablePtr global_context_)
        : DaemonJob{global_context_, CnchBGThreadType::GlobalGC}
    {}
protected:
    bool executeImpl() override;
private:
    Catalog::IMetaStore::IteratorPtr trash_table_it;
    std::vector<DB::Protos::DataModelTable> tables_need_gc;
};

namespace GlobalGCHelpers
{

using ToServerForGCSender = std::function<bool(
    CnchServerClient & client,
    const std::vector<DB::Protos::DataModelTable> & tables_need_gc
)>;

bool sendToServerForGC(
    const std::vector<DB::Protos::DataModelTable> & tables_need_gc,
    std::vector<std::pair<String, long>> & num_of_table_can_send_sorted,
    const std::vector<CnchServerClientPtr> & server_clients,
    ToServerForGCSender sender,
    Poco::Logger * log);

std::vector<std::pair<String, long>> sortByValue(
    std::vector<std::pair<String, long>> && num_of_table_can_send);

}
}
