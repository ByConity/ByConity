#include <Processors/Formats/Impl/MsgPackRowInputFormat.h>

#if USE_MSGPACK

#include <cstdlib>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_END_OF_FILE;
}

MsgPackRowInputFormat::MsgPackRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & settings)
    : MsgPackRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, settings) {}

MsgPackRowInputFormat::MsgPackRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, Params params_, const FormatSettings & settings)
    : IRowInputFormat(header_, *buf_, std::move(params_)), buf(std::move(buf_)), visitor(settings.null_as_default), parser(visitor), data_types(header_.getDataTypes())  {}

void MsgPackRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    buf->reset();
    visitor.reset();
}

void MsgPackVisitor::set_info(IColumn & column, DataTypePtr type, UInt8 & read) // NOLINT
{
    while (!info_stack.empty())
    {
        info_stack.pop();
    }
    info_stack.push(Info{column, type, false, std::nullopt, &read});
}

void MsgPackVisitor::reset()
{
    info_stack = {};
}

template <typename InsertFunc>
static bool checkAndInsertNullable(IColumn & column, DataTypePtr type, InsertFunc insert_func)
{
    if (type->isNullable())
    {
        auto & nullable_column = assert_cast<ColumnNullable &>(column);
        auto & nested_column = nullable_column.getNestedColumn();
        const auto & nested_type = assert_cast<const DataTypeNullable *>(type.get())->getNestedType();
        insert_func(nested_column, nested_type);
        nullable_column.getNullMapColumn().insertValue(0);
        return true;
    }

    return false;
}

template <typename InsertFunc>
static bool checkAndInsertLowCardinality(IColumn & column, DataTypePtr type, InsertFunc insert_func)
{
    if (type->lowCardinality())
    {
        auto & lc_column = assert_cast<ColumnLowCardinality &>(column);
        auto tmp_column = lc_column.getDictionary().getNestedColumn()->cloneEmpty();
        auto dict_type = assert_cast<const DataTypeLowCardinality *>(type.get())->getDictionaryType();
        insert_func(*tmp_column, dict_type);
        lc_column.insertFromFullColumn(*tmp_column, 0);
        return true;
    }
    return false;
}

static void insertInteger(IColumn & column, DataTypePtr type, UInt64 value)
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertInteger(column_, type_, value);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    switch (type->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            assert_cast<ColumnUInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            assert_cast<ColumnUInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value));
            break;
        }
        case TypeIndex::UInt64:
        {
            assert_cast<ColumnUInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            assert_cast<ColumnInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            assert_cast<ColumnInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Int32:
        {
            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(value));
            break;
        }
        case TypeIndex::Int64:
        {
            assert_cast<ColumnInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime64:
        {
            assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(value);
            break;
        }
        case TypeIndex::IPv4:
        {
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(static_cast<UInt32>(value)));
            break;
        }
        case TypeIndex::Decimal32:
        {
            assert_cast<ColumnDecimal<Decimal32> &>(column).insertValue(static_cast<Int32>(value));
            break;
        }
        case TypeIndex::Decimal64:
        {
            assert_cast<ColumnDecimal<Decimal64> &>(column).insertValue(value);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack integer into column with type {}.", type->getName());
    }
}

template <typename ColumnType>
static void insertFromBinaryRepresentation(IColumn & column, DataTypePtr type, const char * value, size_t size)
{
    if (size != sizeof(typename ColumnType::ValueType))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", type->getName(), size);

    assert_cast<ColumnType &>(column).insertData(value, size);
}

static void insertString(IColumn & column, DataTypePtr type, const char * value, size_t size, bool bin)
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertString(column_, type_, value, size, bin);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (isUUID(type))
    {
        ReadBufferFromMemory buf(value, size);
        UUID uuid;
        if (bin)
            readBinary(uuid, buf);
        else
            readUUIDText(uuid, buf);

        assert_cast<ColumnUUID &>(column).insertValue(uuid);
        return;
    }

    if (bin)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::IPv6:
                insertFromBinaryRepresentation<ColumnIPv6>(column, type, value, size);
                return;
            case TypeIndex::Int128:
                insertFromBinaryRepresentation<ColumnInt128>(column, type, value, size);
                return;
            case TypeIndex::UInt128:
                insertFromBinaryRepresentation<ColumnUInt128>(column, type, value, size);
                return;
            case TypeIndex::Int256:
                insertFromBinaryRepresentation<ColumnInt256>(column, type, value, size);
                return;
            case TypeIndex::UInt256:
                insertFromBinaryRepresentation<ColumnUInt256>(column, type, value, size);
                return;
            case TypeIndex::Decimal128:
                insertFromBinaryRepresentation<ColumnDecimal<Decimal128>>(column, type, value, size);
                return;
            case TypeIndex::Decimal256:
                insertFromBinaryRepresentation<ColumnDecimal<Decimal256>>(column, type, value, size);
                return;
            default:;
        }
    }

    if (!isStringOrFixedString(type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack string into column with type {}.", type->getName());

    column.insertData(value, size);
}

static void insertFloat32(IColumn & column, DataTypePtr type, Float32 value) // NOLINT
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertFloat32(column_, type_, value);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!WhichDataType(type).isFloat32())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack float32 into column with type {}.", type->getName());

    assert_cast<ColumnFloat32 &>(column).insertValue(value);
}

static void insertFloat64(IColumn & column, DataTypePtr type, Float64 value) // NOLINT
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertFloat64(column_, type_, value);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!WhichDataType(type).isFloat64())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack float64 into column with type {}.", type->getName());

    assert_cast<ColumnFloat64 &>(column).insertValue(value);
}

static void insertNull(IColumn & column, DataTypePtr type, UInt8 * read, bool null_as_default)
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertNull(column_, type_, read, null_as_default);
    };

    /// LowCardinality(Nullable(...))
    if (checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!type->isNullable())
    {
        if (!null_as_default)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack null into non-nullable column with type {}.", type->getName());
        column.insertDefault();
        /// In case of default on null column can have defined DEFAULT expression that should be used.
        if (read)
            *read = false;
        return;
    }

    assert_cast<ColumnNullable &>(column).insertDefault();
}

bool MsgPackVisitor::visit_positive_integer(UInt64 value) // NOLINT
{
    insertInteger(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_negative_integer(Int64 value) // NOLINT
{
    insertInteger(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_str(const char * value, size_t size) // NOLINT
{
    insertString(info_stack.top().column, info_stack.top().type, value, size, false);
    return true;
}

bool MsgPackVisitor::visit_bin(const char * value, size_t size) // NOLINT
{
    insertString(info_stack.top().column, info_stack.top().type, value, size, true);
    return true;
}

bool MsgPackVisitor::visit_float32(Float32 value) // NOLINT
{
    insertFloat32(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_float64(Float64 value) // NOLINT
{
    insertFloat64(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_boolean(bool value)
{
    insertInteger(info_stack.top().column, info_stack.top().type, UInt64(value));
    return true;
}

bool MsgPackVisitor::start_array(size_t size) // NOLINT
{
    if (isArray(info_stack.top().type))
    {
        auto nested_type = assert_cast<const DataTypeArray &>(*info_stack.top().type).getNestedType();
        ColumnArray & column_array = assert_cast<ColumnArray &>(info_stack.top().column);
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        IColumn & nested_column = column_array.getData();
        offsets.push_back(offsets.back() + size);
        if (size > 0)
            info_stack.push(Info{nested_column, nested_type, false, size, nullptr});
    }
    else if (isTuple(info_stack.top().type))
    {
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*info_stack.top().type);
        const auto & nested_types = tuple_type.getElements();
        if (size != nested_types.size())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack array with size {} into Tuple column with {} elements", size, nested_types.size());

        ColumnTuple & column_tuple = assert_cast<ColumnTuple &>(info_stack.top().column);
        /// Push nested columns into stack in reverse order.
        for (ssize_t i = nested_types.size() - 1; i >= 0; --i)
            info_stack.push(Info{column_tuple.getColumn(i), nested_types[i], true, std::nullopt, nullptr});
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack array into column with type {}", info_stack.top().type->getName());
    }

    return true;
}


bool MsgPackVisitor::end_array_item() // NOLINT
{
    if (info_stack.top().is_tuple_element)
        info_stack.pop();
    else
    {
        assert(info_stack.top().array_size.has_value());
        auto & current_array_size = *info_stack.top().array_size;
        --current_array_size;
        if (current_array_size == 0)
            info_stack.pop();
    }
    return true;
}

bool MsgPackVisitor::start_map(uint32_t size) // NOLINT
{
    if (!isMap(info_stack.top().type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack map into column with type {}.", info_stack.top().type->getName());
    ColumnArray & column_array = assert_cast<ColumnMap &>(info_stack.top().column).getNestedColumn();
    ColumnArray::Offsets & offsets = column_array.getOffsets();
    offsets.push_back(offsets.back() + size);
    return true;
}

bool MsgPackVisitor::start_map_key() // NOLINT
{
    auto key_column = assert_cast<ColumnMap &>(info_stack.top().column).getNestedData().getColumns()[0];
    auto key_type = assert_cast<const DataTypeMap &>(*info_stack.top().type).getKeyType();
    info_stack.push(Info{*key_column, key_type, false, std::nullopt, nullptr});
    return true;
}

bool MsgPackVisitor::end_map_key() // NOLINT
{
    info_stack.pop();
    return true;
}

bool MsgPackVisitor::start_map_value() // NOLINT
{
    auto value_column = assert_cast<ColumnMap &>(info_stack.top().column).getNestedData().getColumns()[1];
    auto value_type = assert_cast<const DataTypeMap &>(*info_stack.top().type).getValueType();
    info_stack.push(Info{*value_column, value_type, false, std::nullopt, nullptr});
    return true;
}

bool MsgPackVisitor::end_map_value() // NOLINT
{
    info_stack.pop();
    return true;
}

bool MsgPackVisitor::visit_nil()
{
    insertNull(info_stack.top().column, info_stack.top().type, info_stack.top().read, null_as_default);
    return true;
}

void MsgPackVisitor::parse_error(size_t, size_t) // NOLINT
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Error occurred while parsing msgpack data.");
}

bool MsgPackRowInputFormat::readObject()
{
    if (buf->eof())
        return false;

    PeekableReadBufferCheckpoint checkpoint{*buf};
    size_t offset = 0;
    while (!parser.execute(buf->position(), buf->available(), offset))
    {
        buf->position() = buf->buffer().end();
        if (buf->eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of file while parsing msgpack object.");
        buf->position() = buf->buffer().end();
        buf->makeContinuousMemoryFromCheckpointToPos();
        buf->rollbackToCheckpoint();
    }
    buf->position() += offset;
    return true;
}

bool MsgPackRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    size_t column_index = 0;
    bool has_more_data = true;
    ext.read_columns.resize(columns.size(), true);
    for (; column_index != columns.size(); ++column_index)
    {
        visitor.set_info(*columns[column_index], data_types[column_index], ext.read_columns[column_index]);
        has_more_data = readObject();
        if (!has_more_data)
            break;
    }
    if (!has_more_data)
    {
        if (column_index != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not enough values to complete the row.");
        return false;
    }
    return true;
}

void registerInputFormatProcessorMsgPack(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("MsgPack", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings & settings)
    {
        return std::make_shared<MsgPackRowInputFormat>(sample, buf, params, settings);
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorMsgPack(FormatFactory &)
{
}
}

#endif
