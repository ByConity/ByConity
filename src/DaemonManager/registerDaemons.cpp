#include <DaemonManager/DaemonFactory.h>
#include <DaemonManager/registerDaemons.h>

namespace DB::DaemonManager
{
void registerServerBGThreads(DaemonFactory & factory);
void registerGlobalGCDaemon(DaemonFactory & factory);
void registerTxnGCDaemon(DaemonFactory & factory);

void registerDaemonJobs()
{
    auto & factory = DaemonFactory::instance();

    registerServerBGThreads(factory);
    registerGlobalGCDaemon(factory);
    registerTxnGCDaemon(factory);
}

}
