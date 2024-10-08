#pragma once

#include <Common/Logger.h>
#include <memory>

#include <DataStreams/IBlockInputStream.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

/** Provides reading from a Buffer, taking exclusive ownership over it's lifetime,
  *    simplifies usage of ReadBufferFromFile (no need to manage buffer lifetime) etc.
  */
template <typename OwnType>
class OwningBlockInputStream : public IBlockInputStream
{
public:
    OwningBlockInputStream(const BlockInputStreamPtr & stream_, std::unique_ptr<OwnType> own_) : own{std::move(own_)}, stream{stream_}
    {
        children.push_back(stream);
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    ~OwningBlockInputStream() override
    {
        children.clear();
        if (stream.use_count() > 1)
            LOG_WARNING(getLogger("OwningBlockInputStream"), "The BlockInputStream might outlive the buffer!");
        if (stream)
            stream.reset();
        if (own)
            own.reset();
    }

private:
    Block readImpl() override { return stream->read(); }

    String getName() const override { return "Owning"; }

protected:
    std::unique_ptr<OwnType> own = nullptr;
    BlockInputStreamPtr stream;
};

}
