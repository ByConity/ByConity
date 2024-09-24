#include "ParquetLeafColReader.h"

#include <utility>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NumberTraits.h>
#include <IO/ReadBufferFromMemory.h>

#include <arrow/util/bit_util.h>
#include "Common/ProfileEvents.h"
#include <common/logger_useful.h>
#include "Columns/ColumnsCommon.h"
#include "Core/Types.h"
#include "Interpreters/castColumn.h"
#include "Processors/Formats/Impl/Parquet/ParquetDataValuesReader.h"
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

namespace ProfileEvents
{
    extern const Event ParquetPrewhereSkippedPageRows;
    extern const Event ParquetGetDataPageElapsedMicroseconds;
    extern const Event ParquetDecodeColumnElapsedMicroseconds;
    extern const Event ParquetDegradeDictionaryElapsedMicroseconds;
    extern const Event ParquetPrewhereSkippedPages;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int PARQUET_EXCEPTION;
}

namespace
{

template <typename TypeVisitor>
void visitColStrIndexType(size_t data_size, TypeVisitor && visitor)
{
    // refer to: DataTypeLowCardinality::createColumnUniqueImpl
    if (data_size < (1ull << 8))
    {
        visitor(static_cast<ColumnUInt8 *>(nullptr));
    }
    else if (data_size < (1ull << 16))
    {
        visitor(static_cast<ColumnUInt16 *>(nullptr));
    }
    else if (data_size < (1ull << 32))
    {
        visitor(static_cast<ColumnUInt32 *>(nullptr));
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported data size {}", data_size);
    }
}

void reserveColumnStrRows(MutableColumnPtr & col, UInt64 rows_num)
{
    col->reserve(rows_num);

    /// Never reserve for too big size according to SerializationString::deserializeBinaryBulk
    if (rows_num < 256 * 1024 * 1024)
    {
        try
        {
            static_cast<ColumnString *>(col.get())->getChars().reserve(rows_num);
        }
        catch (Exception & e)
        {
            e.addMessage("(limit = " + toString(rows_num) + ")");
            throw;
        }
    }
};


template <typename TColumn>
ColumnPtr readDictPage(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & col_des,
    const DataTypePtr & /* data_type */);

template <>
ColumnPtr readDictPage<ColumnString>(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & /* col_des */,
    const DataTypePtr & /* data_type */)
{
    ParquetDataBuffer buffer(page.data(), page.size());

    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();

    /// n values + n additional '0' + 1 empty string
    size_t char_size = buffer.peekStringSize(page.num_values()) + page.num_values() + 1;
    chars.reserve(char_size);
    offsets.resize(page.num_values() + 1);

    // will be read as low cardinality column
    // in which case, the null key is set to first position, so the first string should be empty
    chars.push_back(0);
    offsets[0] = 1;

    for (auto i = 1; i <= page.num_values(); i++)
    {
        buffer.readString(*col, i);
    }
    return col;
}

template <>
ColumnPtr readDictPage<ColumnDecimal<DateTime64>>(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & /* col_des */,
    const DataTypePtr & data_type)
{
    const auto & datetime_type = assert_cast<const DataTypeDateTime64 &>(*data_type);
    auto dict_col = ColumnDecimal<DateTime64>::create(page.num_values(), datetime_type.getScale());
    auto * col_data = dict_col->getData().data();
    ParquetDataBuffer buffer(page.data(), page.size(), datetime_type.getScale());
    for (auto i = 0; i < page.num_values(); i++)
    {
        buffer.readDateTime64(col_data[i]);
    }
    return dict_col;
}

template <is_col_decimal TColumnDecimal>
requires (!std::is_same_v<typename TColumnDecimal::ValueType, DateTime64>)
ColumnPtr readDictPage(const parquet::DictionaryPage & page, const parquet::ColumnDescriptor & col_des, const DataTypePtr & /* data_type */)
{
    auto dict_col = TColumnDecimal::create(page.num_values(), col_des.type_scale());
    ParquetDataBuffer buffer(page.data(), page.size());
    auto * col_data = dict_col->getData().data();
    if (col_des.physical_type() == parquet::Type::FIXED_LEN_BYTE_ARRAY)
    {
        /// big endian
        for (auto i = 0; i < page.num_values(); i++)
            buffer.readBigEndianDecimal(col_data + i, col_des.type_length());
    }
    else
    {
        buffer.readBytes(col_data, page.num_values() * sizeof(typename TColumnDecimal::ValueType));
    }

    return dict_col;
}

template <is_col_vector TColumnVector>
ColumnPtr readDictPage(
    const parquet::DictionaryPage & page,
    const parquet::ColumnDescriptor & /* col_des */,
    const DataTypePtr & /* data_type */)
{
    auto dict_col = TColumnVector::create(page.num_values());
    ParquetDataBuffer buffer(page.data(), page.size());
    if constexpr (std::is_same_v<TColumnVector, ColumnUInt8>)
        buffer.readBits(dict_col->getData().data(), page.num_values());
    else
        buffer.readBytes(dict_col->getData().data(), page.num_values() * sizeof(typename TColumnVector::ValueType));
    return dict_col;
}


template <typename TColumn>
std::unique_ptr<ParquetDataValuesReader> createPlainReader(
    const parquet::ColumnDescriptor & col_des,
    RleValuesReaderPtr def_level_reader,
    ParquetDataBuffer buffer);

template <is_col_decimal TColumnDecimal>
    requires(!std::is_same_v<typename TColumnDecimal::ValueType, DateTime64>)
std::unique_ptr<ParquetDataValuesReader>
createPlainReader(const parquet::ColumnDescriptor & col_des, RleValuesReaderPtr def_level_reader, ParquetDataBuffer buffer)
{
    if constexpr (is_col_over_big_decimal<TColumnDecimal>)
    {
        return std::make_unique<ParquetFixedLenPlainReader<TColumnDecimal>>(
            col_des.max_definition_level(), col_des.type_length(), std::move(def_level_reader), std::move(buffer));
    }
    else
    {
        if (col_des.physical_type() == parquet::Type::FIXED_LEN_BYTE_ARRAY)
            return std::make_unique<ParquetFixedLenPlainReader<TColumnDecimal>>(
                col_des.max_definition_level(), col_des.type_length(), std::move(def_level_reader), std::move(buffer));
        else
            return std::make_unique<ParquetPlainValuesReader<TColumnDecimal>>(
                col_des.max_definition_level(), std::move(def_level_reader), std::move(buffer));
    }
}

template <typename TColumn>
std::unique_ptr<ParquetDataValuesReader> createPlainReader(
    const parquet::ColumnDescriptor & col_des,
    RleValuesReaderPtr def_level_reader,
    ParquetDataBuffer buffer)
{
    return std::make_unique<ParquetPlainValuesReader<TColumn>>(
        col_des.max_definition_level(), std::move(def_level_reader), std::move(buffer));
}


} // anonymous namespace


template <typename TColumn>
ParquetLeafColReader<TColumn>::ParquetLeafColReader(
    const parquet::ColumnDescriptor & col_descriptor_,
    DataTypePtr base_type_,
    std::unique_ptr<parquet::ColumnChunkMetaData> meta_,
    std::unique_ptr<parquet::PageReader> reader_)
    : col_descriptor(col_descriptor_)
    , base_data_type(base_type_)
    , col_chunk_meta(std::move(meta_))
    , parquet_page_reader(std::move(reader_))
    , log(&Poco::Logger::get("ParquetLeafColReader"))
{
}

template <typename TColumn>
ColumnWithTypeAndName ParquetLeafColReader<TColumn>::readBatch(const String & column_name, size_t num_values, const IColumn::Filter * filter)
{
    batch_row_count = num_values;
    cur_row_num = 0;
    resetColumn();

    parquet_page_reader->set_data_page_filter([this, &filter](const parquet::DataPageStats & stats) -> bool {
        size_t values_in_page = stats.num_values;
        if (filter && cur_row_num + values_in_page <= batch_row_count
            && DB::memoryIsZero(filter->data() + cur_row_num, values_in_page))
        {
            ProfileEvents::increment(ProfileEvents::ParquetPrewhereSkippedPageRows, values_in_page);
            ProfileEvents::increment(ProfileEvents::ParquetPrewhereSkippedPages);

            if (!column)
                resetColumn(batch_row_count);

            column->insertManyDefaults(values_in_page);
            cur_row_num += values_in_page;
            return true;
        }
        return false;
    });

    while (cur_row_num < batch_row_count)
    {
        // if dictionary page encountered, another page should be read
        nextDataPage();

        Stopwatch watch;
        auto read_values = std::min(batch_row_count - cur_row_num, num_values_remaining_in_page);

        if (read_values)
        {
            if (!column)
                resetColumn(batch_row_count);
            data_values_reader->readBatch(column, *null_map, read_values);
        }

        num_values_remaining_in_page -= read_values;
        cur_row_num += read_values;
        ProfileEvents::increment(ProfileEvents::ParquetDecodeColumnElapsedMicroseconds, watch.elapsedMicroseconds());
    }
    return releaseColumn(column_name);
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::skip(size_t num_values)
{
    batch_row_count = num_values;
    cur_row_num = 0;
    resetColumn();

    parquet_page_reader->set_data_page_filter([this](const parquet::DataPageStats & stats) -> bool {
        size_t values_in_page = stats.num_values;
        if (cur_row_num + values_in_page <= batch_row_count)
        {
            /// We can skip the entire data page
            cur_row_num += values_in_page;
            return true;
        }
        return false;
    });

    while (cur_row_num < batch_row_count)
    {
        nextDataPage();

        /// skip from current page
        auto skip_values = std::min(batch_row_count - cur_row_num, num_values_remaining_in_page);
        if (skip_values)
            data_values_reader->skip(skip_values);

        num_values_remaining_in_page -= skip_values;
        cur_row_num += skip_values;
    }
}

template <>
void ParquetLeafColReader<ColumnString>::resetColumn(UInt64 rows_num)
{
    if (reading_low_cardinality)
    {
        assert(dictionary);
        visitColStrIndexType(dictionary->size(), [&]<typename TColVec>(TColVec *)
        {
            column = TColVec::create();
        });

        // only first position is used
        null_map = std::make_unique<LazyNullMap>(1);
        column->reserve(rows_num);
    }
    else
    {
        null_map = std::make_unique<LazyNullMap>(rows_num);
        column = ColumnString::create();
        reserveColumnStrRows(column, rows_num);
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::resetColumn(UInt64 rows_num)
{
    assert(!reading_low_cardinality);

    column = base_data_type->createColumn();
    column->reserve(rows_num);
    null_map = std::make_unique<LazyNullMap>(rows_num);
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::resetColumn()
{
    column = nullptr;
    null_map = nullptr;
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::degradeDictionary()
{
    // if last batch read all dictionary indices, then degrade is not needed this time
    if (!column)
    {
        dictionary = nullptr;
        return;
    }
    assert(dictionary && !column->empty());

    Stopwatch watch;

    null_map = std::make_unique<LazyNullMap>(batch_row_count);
    auto col_existing = std::move(column);
    column = ColumnString::create();

    ColumnString & col_dest = *static_cast<ColumnString *>(column.get());
    const ColumnString & col_dict_str = *static_cast<const ColumnString *>(dictionary.get());

    visitColStrIndexType(dictionary->size(), [&]<typename TColVec>(TColVec *)
    {
        const TColVec & col_src = *static_cast<const TColVec *>(col_existing.get());
        reserveColumnStrRows(column, batch_row_count);

        col_dest.getOffsets().resize(col_src.size());
        for (size_t i = 0; i < col_src.size(); i++)
        {
            auto src_idx = col_src.getData()[i];
            if (0 == src_idx)
            {
                null_map->setNull(i);
            }
            auto dict_chars_cursor = col_dict_str.getOffsets()[src_idx - 1];
            auto str_len = col_dict_str.getOffsets()[src_idx] - dict_chars_cursor;
            auto dst_chars_cursor = col_dest.getChars().size();
            col_dest.getChars().resize(dst_chars_cursor + str_len);

            memcpySmallAllowReadWriteOverflow15(
                &col_dest.getChars()[dst_chars_cursor], &col_dict_str.getChars()[dict_chars_cursor], str_len);
            col_dest.getOffsets()[i] = col_dest.getChars().size();
        }
    });
    dictionary = nullptr;

    ProfileEvents::increment(ProfileEvents::ParquetDegradeDictionaryElapsedMicroseconds, watch.elapsedMicroseconds());
    LOG_DEBUG(log, "degraded dictionary to normal column");
}

template <typename TColumn>
ColumnWithTypeAndName ParquetLeafColReader<TColumn>::releaseColumn(const String & name)
{
    DataTypePtr data_type = base_data_type;
    if (reading_low_cardinality)
    {
        MutableColumnPtr col_unique;
        if (null_map->getNullableCol())
        {
            data_type = std::make_shared<DataTypeNullable>(data_type);
            col_unique = ColumnUnique<TColumn>::create(dictionary->assumeMutable(), true);
        }
        else
        {
            col_unique = ColumnUnique<TColumn>::create(dictionary->assumeMutable(), false);
        }
        column = ColumnLowCardinality::create(std::move(col_unique), std::move(column), true);
        data_type = std::make_shared<DataTypeLowCardinality>(data_type);
    }
    else
    {
        if (null_map->getNullableCol())
        {
            column = ColumnNullable::create(std::move(column), null_map->getNullableCol()->assumeMutable());
            data_type = std::make_shared<DataTypeNullable>(data_type);
        }
    }

    ColumnWithTypeAndName res = {std::move(column), data_type, name};
    column = nullptr;
    null_map = nullptr;

    return res;
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::nextDataPage()
{
    while (!num_values_remaining_in_page)
    {
        Stopwatch watch;
        auto page = parquet_page_reader->NextPage();
        ProfileEvents::increment(ProfileEvents::ParquetGetDataPageElapsedMicroseconds, watch.elapsedMicroseconds());

        if (page)
            readPage(*page);
        else
            break;
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPage(const parquet::Page & cur_page)
{
    // refer to: ColumnReaderImplBase::ReadNewPage in column_reader.cc
    // auto cur_page = parquet_page_reader->NextPage();
    switch (cur_page.type())
    {
        case parquet::PageType::DATA_PAGE:
            readPageV1(static_cast<const parquet::DataPageV1 &>(cur_page));
            break;
        case parquet::PageType::DATA_PAGE_V2:
            readPageV2(static_cast<const parquet::DataPageV2 &>(cur_page));
            break;
        case parquet::PageType::DICTIONARY_PAGE:
        {
            readPageDict(static_cast<const parquet::DictionaryPage &>(cur_page));
            break;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported page type: {}", cur_page.type());
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPageV1(const parquet::DataPageV1 & page)
{
    static parquet::LevelDecoder repetition_level_decoder;

    num_values_remaining_in_page = page.num_values();

    // refer to: VectorizedColumnReader::readPageV1 in Spark and LevelDecoder::SetData in column_reader.cc
    if (page.definition_level_encoding() != parquet::Encoding::RLE && col_descriptor.max_definition_level() != 0)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported encoding: {}", page.definition_level_encoding());
    }
    const auto * buffer =  page.data();
    auto max_size = page.size();

    if (col_descriptor.max_repetition_level() > 0)
    {
        auto rep_levels_bytes = repetition_level_decoder.SetData(
            page.repetition_level_encoding(), col_descriptor.max_repetition_level(), 0, buffer, max_size);
        buffer += rep_levels_bytes;
        max_size -= rep_levels_bytes;
    }

    assert(col_descriptor.max_definition_level() >= 0);
    std::unique_ptr<RleValuesReader> def_level_reader;
    if (col_descriptor.max_definition_level() > 0)
    {
        auto bit_width = arrow::bit_util::Log2(col_descriptor.max_definition_level() + 1);
        auto num_bytes = ::arrow::util::SafeLoadAs<int32_t>(buffer);
        auto bit_reader = std::make_unique<arrow::bit_util::BitReader>(buffer + 4, num_bytes);
        num_bytes += 4;
        buffer += num_bytes;
        max_size -= num_bytes;
        def_level_reader = std::make_unique<RleValuesReader>(std::move(bit_reader), bit_width);
    }
    else
    {
        def_level_reader = std::make_unique<RleValuesReader>(page.num_values());
    }

    switch (page.encoding())
    {
        case parquet::Encoding::PLAIN:
        {
            if (reading_low_cardinality)
            {
                reading_low_cardinality = false;
                degradeDictionary();
            }

            ParquetDataBuffer parquet_buffer = [&]()
            {
                if constexpr (!std::is_same_v<ColumnDecimal<DateTime64>, TColumn>)
                    return ParquetDataBuffer(buffer, max_size);

                auto scale = assert_cast<const DataTypeDateTime64 &>(*base_data_type).getScale();
                return ParquetDataBuffer(buffer, max_size, scale);
            }();
            data_values_reader = createPlainReader<TColumn>(
                col_descriptor, std::move(def_level_reader), std::move(parquet_buffer));
            break;
        }
        case parquet::Encoding::RLE_DICTIONARY:
        case parquet::Encoding::PLAIN_DICTIONARY:
        {
            if (unlikely(!dictionary))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "dictionary should be existed");
            }

            // refer to: DictDecoderImpl::SetData in encoding.cc
            auto bit_width = *buffer;
            auto bit_reader = std::make_unique<arrow::bit_util::BitReader>(++buffer, --max_size);
            data_values_reader = createDictReader(
                std::move(def_level_reader), std::make_unique<RleValuesReader>(std::move(bit_reader), bit_width));
            break;
        }
        case parquet::Encoding::BYTE_STREAM_SPLIT:
        case parquet::Encoding::DELTA_BINARY_PACKED:
        case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        case parquet::Encoding::DELTA_BYTE_ARRAY:
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported encoding: {}", page.encoding());

        default:
          throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unknown encoding type: {}", page.encoding());
    }
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPageV2(const parquet::DataPageV2 & /*page*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "read page V2 is not implemented yet");
}

template <typename TColumn>
void ParquetLeafColReader<TColumn>::readPageDict(const parquet::DictionaryPage & dict_page)
{
    num_values_remaining_in_page = 0;

    if (unlikely(
        dict_page.encoding() != parquet::Encoding::PLAIN_DICTIONARY
        && dict_page.encoding() != parquet::Encoding::PLAIN))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Unsupported dictionary page encoding {}", dict_page.encoding());
    }
    // LOG_DEBUG(log, "{} values in dictionary page of column {}", dict_page.num_values(), col_descriptor.name());

    dictionary = readDictPage<TColumn>(dict_page, col_descriptor, base_data_type);
    if (std::is_same_v<TColumn, ColumnString>)
    {
        reading_low_cardinality = true;
    }
}

template <typename TColumn>
std::unique_ptr<ParquetDataValuesReader> ParquetLeafColReader<TColumn>::createDictReader(
    std::unique_ptr<RleValuesReader> def_level_reader, std::unique_ptr<RleValuesReader> rle_data_reader)
{
    if (reading_low_cardinality && std::same_as<TColumn, ColumnString>)
    {
        std::unique_ptr<ParquetDataValuesReader> res;
        visitColStrIndexType(dictionary->size(), [&]<typename TCol>(TCol *)
        {
            res = std::make_unique<ParquetRleLCReader<TCol>>(
                col_descriptor.max_definition_level(),
                std::move(def_level_reader),
                std::move(rle_data_reader));
        });
        return res;
    }
    return std::make_unique<ParquetRleDictReader<TColumn>>(
        col_descriptor.max_definition_level(),
        std::move(def_level_reader),
        std::move(rle_data_reader),
        *assert_cast<const TColumn *>(dictionary.get()));
}

template class ParquetLeafColReader<ColumnUInt8>;
template class ParquetLeafColReader<ColumnInt32>;
template class ParquetLeafColReader<ColumnInt64>;
template class ParquetLeafColReader<ColumnFloat32>;
template class ParquetLeafColReader<ColumnFloat64>;
template class ParquetLeafColReader<ColumnString>;
template class ParquetLeafColReader<ColumnDecimal<Decimal32>>;
template class ParquetLeafColReader<ColumnDecimal<Decimal64>>;
template class ParquetLeafColReader<ColumnDecimal<Decimal128>>;
template class ParquetLeafColReader<ColumnDecimal<Decimal256>>;
template class ParquetLeafColReader<ColumnDecimal<DateTime64>>;

}
