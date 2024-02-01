#pragma once

#include <memory>
#include <mutex>

#include <Common/CurrentMetrics.h>
#include <Common/File.h>


namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
/// RAII for readonly opened file descriptor.
class OpenedFile
{
public:
    OpenedFile(const std::string & file_name_, int flags_);
    ~OpenedFile();

    /// Close prematurally.
    void close();

    int getFD() const;
    std::string getFileName() const;

private:
    std::string file_name;
    int flags = 0;

    mutable File file;
    mutable std::mutex mutex;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open() const;
};

}
