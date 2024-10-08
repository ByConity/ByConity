#include <Common/Logger.h>

LoggerPtr getLogger(const std::string & name)
{
    static Poco::Logger * root = &Poco::Logger::root();
    return std::make_shared<DB::VirtualLogger>(name, root);
}

LoggerPtr getLogger(Poco::Logger & raw_logger)
{
    return std::make_shared<DB::VirtualLogger>(raw_logger.name(), &raw_logger);
}

LoggerRawPtr getRawLogger(const std::string & name)
{
    return &Poco::Logger::get(name);
}
