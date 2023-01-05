#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_utils.h>

namespace UnitTest
{
void initLogger()
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel());
    if (!Poco::Logger::root().getChannel())
    {
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel("trace");
    }
}
}
