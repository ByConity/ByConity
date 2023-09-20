#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Protos/ReadWriteProtobuf.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/util/delimited_message_util.h>
#include "Common/SipHash.h"
#include <Common/SipHash.h>
#include "DataStreams/SizeLimits.h"
#include "IO/WriteHelpers.h"
#include "Processors/Formats/Impl/ArrowBufferedStreams.h"
#include "Storages/MergeTree/ActiveDataPartSet.h"

namespace DB
{
// TODO: use readBuffer internal api to avoid copy
namespace io = google::protobuf::io;

namespace
{

    class BufferOutputStream : public io::CopyingOutputStream
    {
    public:
        explicit BufferOutputStream(WriteBuffer & buf_) : buf(buf_) { }

        bool Write(const void * buffer, int size) override
        {
            buf.write(reinterpret_cast<const char *>(buffer), size);
            return true;
        }

    private:
        WriteBuffer & buf;
    };

    class BufferInputStream : public io::CopyingInputStream
    {
    public:
        explicit BufferInputStream(ReadBuffer & buf_, int total_size_) : buf(buf_), total_size(total_size_) { }
        int Read(void * buffer, int size) override
        {
            // read
            if (!total_size)
                return -1;
            size = std::min(total_size, size);
            auto n = buf.read(reinterpret_cast<char *>(buffer), size);
            total_size -= n;
            return n;
        }

        int Skip(int count) override
        {
            if (!total_size)
                return -1;
            count = std::min(count, total_size);
            auto n = buf.tryIgnore(count);
            total_size -= n;
            return n;
        }

    private:
        ReadBuffer & buf;
        int total_size;
    };

    class HashOutputStream : public io::CopyingOutputStream
    {
    public:
        explicit HashOutputStream(SipHash & sip_hash_) : sip_hash(sip_hash_) { }

        bool Write(const void * buffer, int size) override
        {
            sip_hash.update(reinterpret_cast<const char *>(buffer), size);
            return true;
        }

    private:
        SipHash & sip_hash;
    };
}

// not used since it is slower, reason investigating
void writeProtobuf(const google::protobuf::Message & proto, WriteBuffer & buf)
{
    size_t size = proto.ByteSizeLong();
    writeBinary(size, buf);
    BufferOutputStream stream(buf);
    io::CopyingOutputStreamAdaptor adaptor(&stream);
    proto.SerializeToZeroCopyStream(&adaptor);
}

// not used since it is slower, reason investigating
void readProtobuf(google::protobuf::Message & proto, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);
    BufferInputStream stream(buf, size);
    io::CopyingInputStreamAdaptor adaptor(&stream);
    proto.ParseFromZeroCopyStream(&adaptor);
}

UInt64 sipHash64Protobuf(const google::protobuf::Message & proto)
{
    SipHash sip_hash;
    {
        HashOutputStream stream(sip_hash);
        io::CopyingOutputStreamAdaptor adaptor(&stream);
        io::CodedOutputStream output(&adaptor);
        output.SetSerializationDeterministic(true);
        proto.SerializeToCodedStream(&output);
    }
    return sip_hash.get64();
}

}