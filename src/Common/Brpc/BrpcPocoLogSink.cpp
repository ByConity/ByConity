#include <Common/Brpc/BrpcPocoLogSink.h>
namespace DB
{
bool BrpcPocoLogSink::OnLogMessage(int severity, const char * file, int line, const butil::StringPiece & content)
{
    std::ostringstream os;
    os << '[' << file << ':' << line << ']' << content << std::endl;
    logger->log(Poco::Message(logger->name(), os.str(), brpc2PocoLogPriority(severity)));
    return true;
}

}
