#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#    include "config_core.h"
#endif


#if USE_MSGPACK

#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>
#include <msgpack.hpp>
#include <stack>

namespace DB
{

class ReadBuffer;

class MsgPackVisitor : public msgpack::null_visitor
{
public:
    MsgPackVisitor(bool null_as_default_) : null_as_default(null_as_default_) {}

    struct Info
    {
        IColumn & column;
        DataTypePtr type;
        bool is_tuple_element;
        std::optional<size_t> array_size;
        UInt8 * read;
    };

    /// These functions are called when parser meets corresponding object in parsed data
    bool visit_positive_integer(UInt64 value);
    bool visit_negative_integer(Int64 value);
    bool visit_float32(Float32 value);
    bool visit_float64(Float64 value);
    bool visit_str(const char * value, size_t size);
    bool visit_bin(const char * value, size_t size);
    bool visit_boolean(bool value);
    bool start_array(size_t size);
    bool end_array_item();
    bool visit_nil();
    bool start_map(uint32_t size);
    bool start_map_key();
    bool end_map_key();
    bool start_map_value();
    bool end_map_value();

    /// This function will be called if error occurs in parsing
    [[noreturn]] void parse_error(size_t parsed_offset, size_t error_offset);

    /// Update info_stack
    void set_info(IColumn & column, DataTypePtr type, UInt8 & read);
    void reset();

private:
    /// Stack is needed to process arrays and maps
    std::stack<Info> info_stack;
    bool null_as_default;
};

class MsgPackRowInputFormat : public IRowInputFormat
{
public:
    MsgPackRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & settings);

    String getName() const override { return "MagPackRowInputFormat"; }
    void resetParser() override;

private:
    MsgPackRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, Params params_, const FormatSettings & settings);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

    bool readObject();

    std::unique_ptr<PeekableReadBuffer> buf;
    MsgPackVisitor visitor;
    msgpack::detail::parse_helper<MsgPackVisitor> parser;
    const DataTypes data_types;
};

}

#endif
