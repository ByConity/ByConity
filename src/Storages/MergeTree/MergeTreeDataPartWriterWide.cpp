#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Interpreters/sortBlock.h>
#include <Storages/IndexFile/FilterPolicy.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Common/Coding.h>
#include <Common/Slice.h>
#include <Common/escapeForFileName.h>

#include <Storages/MergeTree/MergeTreeSuffix.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

namespace
{

/// Get granules for block using index_granularity
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Request to get granules from mark {} but index granularity size is {}", current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;
    /// When our last mark is not finished yet and we have to write rows into it
    if (rows_written_in_last_mark > 0)
    {
        size_t rows_left_in_last_mark = index_granularity.getMarkRows(current_mark) - rows_written_in_last_mark;
        size_t rows_left_in_block = block_rows - current_row;
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, rows_left_in_last_mark),
            .mark_number = current_mark,
            .mark_on_start = false, /// Don't mark this granule because we have already marked it
            .is_complete = (rows_left_in_block >= rows_left_in_last_mark),
        });
        current_row += result.back().rows_to_write;
        current_mark++;
    }

    /// Calculating normal granules for block
    while (current_row < block_rows)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block  = block_rows - current_row;
        /// If we have less rows in block than expected in granularity
        /// save incomplete granule
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (rows_left_in_block >= expected_rows_in_mark),
        });
        current_row += result.back().rows_to_write;
        current_mark++;
    }

    return result;
}

}

MergeTreeDataPartWriterWide::MergeTreeDataPartWriterWide(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : MergeTreeDataPartWriterOnDisk(data_part_, columns_list_, metadata_snapshot_,
           indices_to_recalc_, marks_file_extension_,
           default_codec_, settings_, index_granularity_)
{
    const auto & columns = metadata_snapshot->getColumns();
    for (const auto & it : columns_list)
        addStreams(it, columns.getCodecDescOrDefault(it.name, default_codec));
}


MergeTreeDataPartWriterWide::~MergeTreeDataPartWriterWide()
{
    closeTempUniqueKeyIndex();
}

void MergeTreeDataPartWriterWide::addStreams(
    const NameAndTypePair & column,
    const ASTPtr & effective_codec_desc)
{
    IDataType::StreamCallbackWithType callback = [&] (const ISerialization::SubstreamPath & substream_path, const IDataType & substream_type)
    {
        String stream_name = ISerialization::getFileNameForStream(column, substream_path);
        /// Shared offsets for Nested type.
        if (column_streams.count(stream_name))
            return;

        CompressionCodecPtr compression_codec;
        /// If we can use special codec then just get it
        if (ISerialization::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, &substream_type, default_codec);
        else /// otherwise return only generic codecs and don't use info about the` data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        column_streams[stream_name] = std::make_unique<Stream>(
            stream_name,
            data_part->volume->getDisk(),
            part_path + stream_name, DATA_FILE_EXTENSION,
            part_path + stream_name, marks_file_extension,
            compression_codec,
            settings.max_compress_block_size);
    };

    column.type->enumerateStreams(serializations[column.name], callback);
}


ISerialization::OutputStreamGetter MergeTreeDataPartWriterWide::createStreamGetter(
        const NameAndTypePair & column, WrittenOffsetColumns & offset_columns) const
{
    return [&, this] (const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;

        String stream_name = ISerialization::getFileNameForStream(column, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return nullptr;

        return &column_streams.at(stream_name)->compressed;
    };
}


void MergeTreeDataPartWriterWide::shiftCurrentMark(const Granules & granules_written)
{
    auto last_granule = granules_written.back();
    /// If we didn't finished last granule than we will continue to write it from new block
    if (!last_granule.is_complete)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incomplete granules are not allowed while blocks are granules size. "
                "Mark number {} (rows {}), rows written in last mark {}, rows to write in last mark from block {} (from row {}), total marks currently {}",
                last_granule.mark_number, index_granularity.getMarkRows(last_granule.mark_number), rows_written_in_last_mark,
                last_granule.rows_to_write, last_granule.start_row, index_granularity.getMarksCount());

        /// Shift forward except last granule
        setCurrentMark(getCurrentMark() + granules_written.size() - 1);
        bool still_in_the_same_granule = granules_written.size() == 1;
        /// We wrote whole block in the same granule, but didn't finished it.
        /// So add written rows to rows written in last_mark
        if (still_in_the_same_granule)
            rows_written_in_last_mark += last_granule.rows_to_write;
        else
            rows_written_in_last_mark = last_granule.rows_to_write;
    }
    else
    {
        setCurrentMark(getCurrentMark() + granules_written.size());
        rows_written_in_last_mark = 0;
    }
}

void MergeTreeDataPartWriterWide::write(const Block & block, const IColumn::Permutation * permutation)
{
    size_t rows = block.rows();

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical part of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(block);
        if (rows_written_in_last_mark > 0)
        {
            size_t rows_left_in_last_mark = index_granularity.getMarkRows(getCurrentMark()) - rows_written_in_last_mark;
            /// Previous granularity was much bigger than our new block's
            /// granularity let's adjust it, because we want add new
            /// heavy-weight blocks into small old granule.
            if (rows_left_in_last_mark > index_granularity_for_block)
            {
                /// We have already written more rows than granularity of our block.
                /// adjust last mark rows and flush to disk.
                if (rows_written_in_last_mark >= index_granularity_for_block)
                    adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
                else /// We still can write some rows from new block into previous granule. So the granule size will be block granularity size.
                    adjustLastMarkIfNeedAndFlushToDisk(index_granularity_for_block);
            }
        }

        fillIndexGranularity(index_granularity_for_block, block.rows());
    }

    if (storage.isBitEngineMode())
    {
        /// encode bitmap column by BitEngine dictionary if needed
        writeImplicitColumnForBitEngine(const_cast<Block &>(block));
    }
    auto granules_to_write = getGranulesToWrite(index_granularity, block.rows(), getCurrentMark(), rows_written_in_last_mark);

    auto offset_columns = written_offset_columns ? *written_offset_columns : WrittenOffsetColumns{};
    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block skip_indexes_block = getBlockAndPermute(block, getSkipIndicesColumns(), permutation);

    /// contains permuted underlying columns for unique key
    Block unique_key_block;
    NameSet unique_key_underlying_columns;
    String extra_column_name;
    size_t extra_column_size = 0;
    if (enable_disk_based_key_index)
    {
        auto required_columns = metadata_snapshot->getColumnsRequiredForUniqueKey();
        unique_key_underlying_columns.insert(required_columns.begin(), required_columns.end());
        extra_column_name = metadata_snapshot->extra_column_name;
        extra_column_size = metadata_snapshot->extra_column_size;
    }

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        const ColumnWithTypeAndName & column = block.getByName(it->name);
        const bool part_of_unique_key = unique_key_underlying_columns.count(column.name) > 0;
        const bool is_extra_column = !extra_column_name.empty() && (column.name == extra_column_name);

        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                const auto & primary_column = primary_key_block.getByName(it->name);
                if (part_of_unique_key || is_extra_column)
                    unique_key_block.insert(primary_column);
                writeColumn(*it, *primary_column.column, offset_columns, granules_to_write);
            }
            else if (skip_indexes_block.has(it->name))
            {
                const auto & index_column = skip_indexes_block.getByName(it->name);
                if (part_of_unique_key || is_extra_column)
                    unique_key_block.insertUnique(index_column);
                writeColumn(*it, *index_column.column, offset_columns, granules_to_write);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                if (part_of_unique_key || is_extra_column)
                    unique_key_block.insert(ColumnWithTypeAndName(permuted_column, column.type, column.name));
                writeColumn(*it, *permuted_column, offset_columns, granules_to_write);
            }
        }
        else
        {
            if (part_of_unique_key || is_extra_column)
                unique_key_block.insert(column);
            writeColumn(*it, *column.column, offset_columns, granules_to_write);
        }
    }

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);

    calculateAndSerializeSkipIndices(skip_indexes_block, granules_to_write);

    shiftCurrentMark(granules_to_write);

    // FIXME (UNIQUE KEY): Make this into a seperate funnction later on
    if (enable_disk_based_key_index)
    {
        assert(unique_key_block.rows() == rows);
        if (!temp_unique_key_index)
        {
            if (rows_count == 0)
            {
                /// buffer the first block so that we can generate key index file directly from the buffered block later
                buffered_unique_key_block = std::move(unique_key_block);
            }
            else if (rows > 0)
            {
                /// merge case (more than one block) : convert buffered block into "temp_unique_key_index"
                assert(buffered_unique_key_block.rows() > 0);
                rocksdb::Options opts;
                opts.create_if_missing = true;
                opts.error_if_exists = true;
                opts.write_buffer_size = 16 << 20; /// 16MB
                temp_unique_key_index_dir = data_part->getFullPath() + "TEMP_unique_key_index";
                auto status = rocksdb::DB::Open(opts, temp_unique_key_index_dir, &temp_unique_key_index);
                if (!status.ok())
                    throw Exception("Can't create temp unique key index at " + temp_unique_key_index_dir, ErrorCodes::LOGICAL_ERROR);
                writeToTempUniqueKeyIndex(buffered_unique_key_block, /*first_rid=*/0, *temp_unique_key_index);
                buffered_unique_key_block.clear();
            }
        }

        if (temp_unique_key_index)
        {
            /// merge case (more than one block) : add index entries to "temp_unique_key_index" first
            writeToTempUniqueKeyIndex(unique_key_block, /*first_rid=*/rows_count, *temp_unique_key_index);
        }
    }

    rows_count += rows;
}

void MergeTreeDataPartWriterWide::writeSingleMark(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    size_t number_of_rows,
    ISerialization::SubstreamPath & path)
{
    StreamsWithMarks marks = getCurrentMarksForColumn(column, offset_columns, path);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterWide::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    Stream & stream = *column_streams[stream_with_mark.stream_name];
    writeIntBinary(stream_with_mark.mark.offset_in_compressed_file, stream.marks);
    writeIntBinary(stream_with_mark.mark.offset_in_decompressed_block, stream.marks);
    if (settings.can_use_adaptive_granularity)
        writeIntBinary(rows_in_mark, stream.marks);
}

StreamsWithMarks MergeTreeDataPartWriterWide::getCurrentMarksForColumn(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    ISerialization::SubstreamPath & path)
{
    StreamsWithMarks result;
    serializations[column.name]->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;

        String stream_name = ISerialization::getFileNameForStream(column, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        Stream & stream = *column_streams[stream_name];

        /// There could already be enough data to compress into the new block.
        if (stream.compressed.offset() >= settings.min_compress_block_size)
            stream.compressed.next();

        StreamNameAndMark stream_with_mark;
        stream_with_mark.stream_name = stream_name;
        stream_with_mark.mark.offset_in_compressed_file = stream.plain_hashing.count();
        stream_with_mark.mark.offset_in_decompressed_block = stream.compressed.offset();

        result.push_back(stream_with_mark);
    }, path);

    return result;
}

void MergeTreeDataPartWriterWide::writeSingleGranule(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    ISerialization::SerializeBinaryBulkStatePtr & serialization_state,
    ISerialization::SerializeBinaryBulkSettings & serialize_settings,
    const Granule & granule)
{
    const auto & serialization = serializations[name_and_type.name];
    serialization->serializeBinaryBulkWithMultipleStreams(column, granule.start_row, granule.rows_to_write, serialize_settings, serialization_state);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;

        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        column_streams[stream_name]->compressed.nextIfAtEnd();
    }, serialize_settings.path);
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterWide::writeColumn(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    const Granules & granules)
{
    if (granules.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty granules for column {}, current mark {}", backQuoteIfNeed(name_and_type.name), getCurrentMark());

    const auto & [name, type] = name_and_type;
    auto [it, inserted] = serialization_states.emplace(name, nullptr);

    if (inserted)
    {
        ISerialization::SerializeBinaryBulkSettings serialize_settings;
        serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
        serializations[name]->serializeBinaryBulkStatePrefix(serialize_settings, it->second);
    }

    const auto & global_settings = storage.getContext()->getSettingsRef();
    ISerialization::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;

    for (const auto & granule : granules)
    {
        data_written = true;

        if (granule.mark_on_start)
        {
            if (last_non_written_marks.count(name))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "We have to add new mark for column, but already have non written mark. Current mark {}, total marks {}, offset {}", getCurrentMark(), index_granularity.getMarksCount(), rows_written_in_last_mark);
            last_non_written_marks[name] = getCurrentMarksForColumn(name_and_type, offset_columns, serialize_settings.path);
        }

        writeSingleGranule(
           name_and_type,
           column,
           offset_columns,
           it->second,
           serialize_settings,
           granule
        );

        if (granule.is_complete)
        {
            auto marks_it = last_non_written_marks.find(name);
            if (marks_it == last_non_written_marks.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No mark was saved for incomplete granule for column {}", backQuoteIfNeed(name));

            for (const auto & mark : marks_it->second)
                flushMarkToFile(mark, index_granularity.getMarkRows(granule.mark_number));
            last_non_written_marks.erase(marks_it);
        }
    }

    serializations[name]->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);
            offset_columns.insert(stream_name);
        }
    }, serialize_settings.path);
}


void MergeTreeDataPartWriterWide::validateColumnOfFixedSize(const String & name, const IDataType & type)
{
    if (!type.isValueRepresentedByNumber() || type.haveSubtypes())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate column of non fixed type {}", type.getName());

    auto disk = data_part->volume->getDisk();
    String escaped_name = escapeForFileName(name);
    String mrk_path = fullPath(disk, part_path + escaped_name + marks_file_extension);
    String bin_path = fullPath(disk, part_path + escaped_name + DATA_FILE_EXTENSION);
    DB::ReadBufferFromFile mrk_in(mrk_path);
    DB::CompressedReadBufferFromFile bin_in(bin_path, 0, 0, 0, nullptr);
    bool must_be_last = false;
    UInt64 offset_in_compressed_file = 0;
    UInt64 offset_in_decompressed_block = 0;
    UInt64 index_granularity_rows = data_part->index_granularity_info.fixed_index_granularity;

    size_t mark_num;

    const auto & serialization = serializations[name];
    for (mark_num = 0; !mrk_in.eof(); ++mark_num)
    {
        if (mark_num > index_granularity.getMarksCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect number of marks in memory {}, on disk (at least) {}", index_granularity.getMarksCount(), mark_num + 1);

        DB::readBinary(offset_in_compressed_file, mrk_in);
        DB::readBinary(offset_in_decompressed_block, mrk_in);
        if (settings.can_use_adaptive_granularity)
            DB::readBinary(index_granularity_rows, mrk_in);
        else
            index_granularity_rows = data_part->index_granularity_info.fixed_index_granularity;

        if (must_be_last)
        {
            if (index_granularity_rows != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "We ran out of binary data but still have non empty mark #{} with rows number {}", mark_num, index_granularity_rows);

            if (!mrk_in.eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark #{} must be last, but we still have some to read", mark_num);

            break;
        }

        if (index_granularity_rows == 0)
        {
            auto column = type.createColumn();

            serialization->deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

            throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have {} rows in bin stream, last mark #{} index granularity size {}, last rows {}", column->size(), mark_num, index_granularity.getMarksCount(), index_granularity_rows);
        }

        if (index_granularity_rows > data_part->index_granularity_info.fixed_index_granularity)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Mark #{} has {} rows, but max fixed granularity is {}, index granularity size {}",
                            mark_num, index_granularity_rows, data_part->index_granularity_info.fixed_index_granularity, index_granularity.getMarksCount());
        }

        if (index_granularity_rows != index_granularity.getMarkRows(mark_num))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Incorrect mark rows for part {} for mark #{} (compressed offset {}, decompressed offset {}), in-memory {}, on disk {}, total marks {}",
                data_part->getFullPath(), mark_num, offset_in_compressed_file, offset_in_decompressed_block, index_granularity.getMarkRows(mark_num), index_granularity_rows, index_granularity.getMarksCount());

        auto column = type.createColumn();

        serialization->deserializeBinaryBulk(*column, bin_in, index_granularity_rows, 0.0);

        if (bin_in.eof())
        {
            must_be_last = true;
        }

        /// Now they must be equal
        if (column->size() != index_granularity_rows)
        {

            if (must_be_last)
            {
                /// The only possible mark after bin.eof() is final mark. When we
                /// cannot use adaptive granularity we cannot have last mark.
                /// So finish validation.
                if (!settings.can_use_adaptive_granularity)
                    break;

                /// If we don't compute granularity then we are not responsible
                /// for last mark (for example we mutating some column from part
                /// with fixed granularity where last mark is not adjusted)
                if (!compute_granularity)
                    continue;
            }

            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Incorrect mark rows for mark #{} (compressed offset {}, decompressed offset {}), actually in bin file {}, in mrk file {}, total marks {}",
                mark_num, offset_in_compressed_file, offset_in_decompressed_block, column->size(), index_granularity.getMarkRows(mark_num), index_granularity.getMarksCount());
        }
    }

    if (!mrk_in.eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have something in marks stream, last mark #{} index granularity size {}, last rows {}", mark_num, index_granularity.getMarksCount(), index_granularity_rows);
    if (!bin_in.eof())
    {
        auto column = type.createColumn();

        serialization->deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have {} rows in bin stream, last mark #{} index granularity size {}, last rows {}", column->size(), mark_num, index_granularity.getMarksCount(), index_granularity_rows);
    }

}

void MergeTreeDataPartWriterWide::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    const auto & global_settings = storage.getContext()->getSettingsRef();
    ISerialization::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;
    WrittenOffsetColumns offset_columns;
    if (rows_written_in_last_mark > 0)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incomplete granule is not allowed while blocks are granules size even for last granule. "
                "Mark number {} (rows {}), rows written for last mark {}, total marks {}",
                getCurrentMark(), index_granularity.getMarkRows(getCurrentMark()), rows_written_in_last_mark, index_granularity.getMarksCount());

        adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
    }

    bool write_final_mark = (with_final_mark && data_written);

    {
        auto it = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++it)
        {
            if (!serialization_states.empty())
            {
                serialize_settings.getter = createStreamGetter(*it, written_offset_columns ? *written_offset_columns : offset_columns);
                serializations[it->name]->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[it->name]);
            }

            if (write_final_mark)
                writeFinalMark(*it, offset_columns, serialize_settings.path);
        }
    }
    for (auto & stream : column_streams)
    {
        stream.second->finalize();
        stream.second->addToChecksums(checksums);
        if (sync)
            stream.second->sync();
    }

    column_streams.clear();
    serialization_states.clear();

#ifndef NDEBUG
    /// Heavy weight validation of written data. Checks that we are able to read
    /// data according to marks. Otherwise throws LOGICAL_ERROR (equal to abort in debug mode)
    for (const auto & column : columns_list)
    {
        if (column.type->isValueRepresentedByNumber() && !column.type->haveSubtypes())
            validateColumnOfFixedSize(column.name, *column.type);
    }
#endif

}

void MergeTreeDataPartWriterWide::finish(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        finishDataSerialization(checksums, sync);

    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(checksums, sync);

    finishSkipIndicesSerialization(checksums, sync);

    if (enable_disk_based_key_index)
    {
        IndexFile::IndexFileInfo file_info;
        writeFinalUniqueKeyIndex(file_info);
        /// we don't create index file for empty part
        if (file_info.file_size > 0)
        {
            checksums.files[UKI_FILE_NAME].file_size = file_info.file_size;
            checksums.files[UKI_FILE_NAME].file_hash = file_info.file_hash;
        }
    }
}

void MergeTreeDataPartWriterWide::writeFinalMark(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    ISerialization::SubstreamPath & path)
{
    writeSingleMark(column, offset_columns, 0, path);
    /// Memoize information about offsets
    serializations[column.name]->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = ISerialization::getFileNameForStream(column, substream_path);
            offset_columns.insert(stream_name);
        }
    }, path);
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity,
    size_t index_offset,
    size_t index_granularity_for_block,
    size_t rows_in_block)
{
    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void MergeTreeDataPartWriterWide::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    if (getCurrentMark() < index_granularity.getMarksCount() && getCurrentMark() != index_granularity.getMarksCount() - 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to add marks, while current mark {}, but total marks {}", getCurrentMark(), index_granularity.getMarksCount());

    size_t index_offset = 0;
    if (rows_written_in_last_mark != 0)
        index_offset = index_granularity.getLastMarkRows() - rows_written_in_last_mark;

    fillIndexGranularityImpl(
        index_granularity,
        index_offset,
        index_granularity_for_block,
        rows_in_block);
}


void MergeTreeDataPartWriterWide::adjustLastMarkIfNeedAndFlushToDisk(size_t new_rows_in_last_mark)
{
    /// We don't want to split already written granules to smaller
    if (rows_written_in_last_mark > new_rows_in_last_mark)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tryin to make mark #{} smaller ({} rows) then it already has {}",
                        getCurrentMark(), new_rows_in_last_mark, rows_written_in_last_mark);

    /// We can adjust marks only if we computed granularity for blocks.
    /// Otherwise we cannot change granularity because it will differ from
    /// other columns
    if (compute_granularity && settings.can_use_adaptive_granularity)
    {
        if (getCurrentMark() != index_granularity.getMarksCount() - 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Non last mark {} (with {} rows) having rows offset {}, total marks {}",
                            getCurrentMark(), index_granularity.getMarkRows(getCurrentMark()), rows_written_in_last_mark, index_granularity.getMarksCount());

        index_granularity.popMark();
        index_granularity.appendMark(new_rows_in_last_mark);
    }

    /// Last mark should be filled, otherwise it's a bug
    if (last_non_written_marks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No saved marks for last mark {} having rows offset {}, total marks {}",
                        getCurrentMark(), rows_written_in_last_mark, index_granularity.getMarksCount());

    if (rows_written_in_last_mark == new_rows_in_last_mark)
    {
        for (const auto & [name, marks] : last_non_written_marks)
        {
            for (const auto & mark : marks)
                flushMarkToFile(mark, index_granularity.getMarkRows(getCurrentMark()));
        }

        last_non_written_marks.clear();

        if (compute_granularity && settings.can_use_adaptive_granularity)
        {
            /// Also we add mark to each skip index because all of them
            /// already accumulated all rows from current adjusting mark
            for (size_t i = 0; i < skip_indices.size(); ++i)
                ++skip_index_accumulated_marks[i];

            /// This mark completed, go further
            setCurrentMark(getCurrentMark() + 1);
            /// Without offset
            rows_written_in_last_mark = 0;
        }
    }
}

void MergeTreeDataPartWriterWide::writeToTempUniqueKeyIndex(Block & block, size_t first_rid, rocksdb::DB & temp_index)
{
    auto unique_key_expr = metadata_snapshot->getUniqueKeyExpression();
    unique_key_expr->execute(block);

    ColumnsWithTypeAndName key_columns;
    for (auto & col_name : metadata_snapshot->getUniqueKeyColumns())
        key_columns.emplace_back(block.getByName(col_name));

    rocksdb::WriteOptions opts;
    opts.disableWAL = true;

    String extra_column_name = metadata_snapshot->extra_column_name;
    size_t extra_column_size = metadata_snapshot->extra_column_size;

    const ColumnWithTypeAndName * extra_column = nullptr;
    if (!extra_column_name.empty())
        extra_column = &block.getByName(extra_column_name);

    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        WriteBufferFromOwnString key_buf;
        for (auto & col : key_columns)
        {
            serializations[col.name]->serializeMemComparable(*col.column, i, key_buf);
        }
        String value;
        auto rid = static_cast<UInt32>(first_rid + i);
        PutVarint32(&value, rid);

        if (extra_column)
        {
            /// append extra column data at the end of value
            if (extra_column_size == 8) /// handle explicit version column
            {
                UInt64 version = extra_column->column->getUInt(i);
                value.append(reinterpret_cast<char *>(&version), extra_column_size);
            }
            else if (extra_column_size == 1) /// handle is_offline column
            {
                UInt8 is_offline_value = extra_column->column->getBool(i);
                value.append(reinterpret_cast<char *>(&is_offline_value), extra_column_size);
            }
        }

        auto status = temp_index.Put(opts, key_buf.str(), value);
        if (!status.ok())
            throw Exception("Failed to add unique key : " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
    }
}

void MergeTreeDataPartWriterWide::writeFinalUniqueKeyIndex(IndexFile::IndexFileInfo & file_info)
{
    if (!enable_disk_based_key_index || rows_count == 0)
        return;

    /// write unique_key -> rowid mappings to key index file
    String unique_key_index_file = data_part->getFullPath() + UKI_FILE_NAME;
    IndexFile::Options options;
    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
    IndexFile::IndexFileWriter index_writer(options);
    auto status = index_writer.Open(unique_key_index_file);
    if (!status.ok())
        throw Exception("Error while opening file " + unique_key_index_file + ": " + status.ToString(), ErrorCodes::CANNOT_OPEN_FILE);

    if (!temp_unique_key_index)
    {
        /// normal insert case : create index file from buffered block
        /// sort by unique key exprs
        auto unique_key_expr = metadata_snapshot->getUniqueKeyExpression();
        unique_key_expr->execute(buffered_unique_key_block);

        SortDescription sort_description;
        auto unique_key_columns = metadata_snapshot->getUniqueKeyColumns();
        sort_description.reserve(unique_key_columns.size());
        for (auto & name : unique_key_columns)
            sort_description.emplace_back(buffered_unique_key_block.getPositionByName(name), 1, 1);

        IColumn::Permutation * unique_key_perm_ptr = nullptr;
        IColumn::Permutation unique_key_perm;
        if (!isAlreadySorted(buffered_unique_key_block, sort_description))
        {
            stableGetPermutation(buffered_unique_key_block, sort_description, unique_key_perm);
            unique_key_perm_ptr = &unique_key_perm;
        }

        ColumnsWithTypeAndName key_columns;
        for (auto & col_name : unique_key_columns)
            key_columns.emplace_back(buffered_unique_key_block.getByName(col_name));

        String extra_column_name = metadata_snapshot->extra_column_name;
        size_t extra_column_size = metadata_snapshot->extra_column_size;

        const ColumnWithTypeAndName * extra_column = nullptr;
        if (!extra_column_name.empty())
            extra_column = &buffered_unique_key_block.getByName(extra_column_name);

        for (UInt32 rid = 0, size = buffered_unique_key_block.rows(); rid < size; ++rid)
        {
            size_t idx = unique_key_perm_ptr ? unique_key_perm[rid] : rid;
            WriteBufferFromOwnString key_buf;
            for (auto & col : key_columns)
            {
                auto serial = col.type->getDefaultSerialization();
                serial->serializeMemComparable(*col.column, idx, key_buf);
            }
            String value;
            PutVarint32(&value, static_cast<UInt32>(idx));

            if (extra_column)
            {
                /// append extra column data at the end of value
                if (extra_column_size == 8) /// handle explicit version column
                {
                    UInt64 version = extra_column->column->getUInt(rid);
                    value.append(reinterpret_cast<char *>(&version), extra_column_size);
                }
                else if (extra_column_size == 1) /// handle is_offline column
                {
                    UInt8 is_offline_value = extra_column->column->getBool(rid);
                    value.append(reinterpret_cast<char *>(&is_offline_value), extra_column_size);
                }
            }

            status = index_writer.Add(key_buf.str(), value);
            if (!status.ok())
                throw Exception("Error while adding key to " + unique_key_index_file + ": " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
        }

        buffered_unique_key_block.clear();
    }
    else
    {
        /// merge case : create index file from temp index
        std::unique_ptr<rocksdb::Iterator> iter(temp_unique_key_index->NewIterator(rocksdb::ReadOptions()));
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            auto key = iter->key();
            auto val = iter->value();
            status = index_writer.Add(Slice(key.data(), key.size()), Slice(val.data(), val.size()));
            if (!status.ok())
                throw Exception("Error while adding key to " + unique_key_index_file + ": " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
        }
        if (!iter->status().ok())
            throw Exception(
                "Error while scanning temp key index file " + temp_unique_key_index_dir + ": " + iter->status().ToString(),
                ErrorCodes::LOGICAL_ERROR);
        iter.reset();
        closeTempUniqueKeyIndex();
    }

    status = index_writer.Finish(&file_info);
    if (!status.ok())
        throw Exception("Error while finishing file " + unique_key_index_file + ": " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeDataPartWriterWide::closeTempUniqueKeyIndex()
{
    if (temp_unique_key_index)
    {
        try
        {
            auto status = temp_unique_key_index->Close();
            if (!status.ok())
                LOG_WARNING(
                    &Poco::Logger::get("MergedBlockOutputStream"), "Failed to close {} : {}", temp_unique_key_index_dir, status.ToString());
            delete temp_unique_key_index;
            temp_unique_key_index = nullptr;

            Poco::File(temp_unique_key_index_dir).remove(true);
        }
        catch (...)
        {
            LOG_WARNING(&Poco::Logger::get("MergedBlockOutputStream"), "{}", getCurrentExceptionMessage(false));
        }
    }
}
}
