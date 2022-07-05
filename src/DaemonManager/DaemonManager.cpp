#include <iostream>
int mainEntryClickHouseDaemonManager(int /*argc*/, char ** /*argv*/)
{
    std::cout << "hello world\n";
    return 0;
#if 0
    DB::DaemonManager::DaemonManager server;
    try
    {
        return server.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
#endif
}
