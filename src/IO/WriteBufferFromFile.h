#pragma once

#include <sys/types.h>

#include <Common/Throttler.h>
#include <Common/CurrentMetrics.h>
#include <IO/WriteBufferFromFileDescriptor.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForWrite;
}


#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{

/** Accepts path to file and opens it, or pre-opened file descriptor.
  * Closes file by himself (thus "owns" a file descriptor).
  */
class WriteBufferFromFile : public WriteBufferFromFileDescriptor
{
protected:
    std::string file_name;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};

public:
    WriteBufferFromFile(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        ThrottlerPtr throttler = nullptr);

    /// Use pre-opened file descriptor.
    WriteBufferFromFile(
        int & fd,   /// Will be set to -1 if constructor didn't throw and ownership of file descriptor is passed to the object.
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromFile() override;

    /// Close file before destruction of object.
    void close();

    std::string getFileName() const override
    {
        return file_name;
    }

    WriteBuffer * inplaceReconstruct(const String & out_path, [[maybe_unused]] std::unique_ptr<WriteBuffer> nested) override
    {
        // Call the destructor explicitly but does not free memory
        this->~WriteBufferFromFile();
        new (this) WriteBufferFromFile(out_path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
        return this;
    }
};

}
