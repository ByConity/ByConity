#pragma once

#include <string>
#include <IO/WriteBufferFromVector.h>
#include <common/StringRef.h>


namespace DB
{

/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object.
  */
using WriteBufferFromString = WriteBufferFromVector<std::string>;


namespace detail
{
    /// For correct order of initialization.
    class StringHolder
    {
    protected:
        std::string value;
    };
}

/// Creates the string by itself and allows to get it.
class WriteBufferFromOwnString : public detail::StringHolder, public WriteBufferFromString
{
public:
    WriteBufferFromOwnString() : WriteBufferFromString(value) {}

    StringRef stringRef() const { return isFinished() ? StringRef(value) : StringRef(value.data(), pos - value.data()); }

    std::string & str()
    {
        finalize();
        return value;
    }

    WriteBuffer * inplaceReconstruct([[maybe_unused]] const String & out_path, [[maybe_unused]] std::unique_ptr<WriteBuffer> nested) override
    {
        // Call the destructor explicitly but does not free memory
        this->~WriteBufferFromOwnString();
        new (this) WriteBufferFromOwnString();
        return this;
    }
};

}
