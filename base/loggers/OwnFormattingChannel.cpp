#include "OwnFormattingChannel.h"
#include "OwnPatternFormatter.h"


namespace DB
{

void OwnFormattingChannel::logExtended(const ExtendedLogMessage & msg)
{
    bool query_logs_level_specified = msg.query_logs_level_for_poco != 0;
    bool print_log
        = query_logs_level_specified ? msg.query_logs_level_for_poco >= msg.base.getPriority() : priority >= msg.base.getPriority();
    if (pChannel && print_log)
    {
        if (pFormatter)
        {
            std::string text;
            pFormatter->formatExtended(msg, text);
            pChannel->log(Poco::Message(msg.base, text));
        }
        else
        {
            pChannel->log(msg.base);
        }
    }
}

void OwnFormattingChannel::log(const Poco::Message & msg)
{
    logExtended(ExtendedLogMessage::getFrom(msg));
}

OwnFormattingChannel::~OwnFormattingChannel() = default;

}
