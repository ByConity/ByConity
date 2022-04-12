#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

#include <Columns/ColumnByteMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeByteMap.h>
#include <DataTypes/MapHelpers.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnByteMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/FieldVisitorToString.h>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
//constexpr auto INDEX_FILE_EXTENSION = ".idx";

static void constructImplicitValueColumns(
    const ColumnByteMap & column_map,
    std::unordered_map<StringRef, String> & key_name_map,
    std::unordered_map<StringRef, ColumnPtr> & value_columns)
{
    /// Stopwatch stopwatch;

    /// Construct value columns to write
    auto & column_map_key = column_map.getKey();
    auto & column_map_value = column_map.getValue();
    auto & column_offsets = column_map.getOffsets();
    const size_t num_map_rows = column_map.size();

    /// This is an one-pass algorithm, to minimize the cost of traversing all key-value pairs:
    /// 1) Names of key and implicit value columns are created lazily;
    /// 2) NULLs are filled as need before non-null values inserted or in the end.
    for (size_t r = 0; r < num_map_rows; ++r)
    {
        const size_t offset = column_offsets[r - 1]; /// -1th index is Ok, see PaddedPODArray
        const size_t curr_num_pair = column_offsets[r] - offset;

        for (size_t p = 0; p < curr_num_pair; ++p)
        {
            auto tmp_key = column_map_key.getDataAt(offset + p);
            auto iter = value_columns.find(tmp_key);

            if (iter == value_columns.end())
            {
                key_name_map[tmp_key] = applyVisitor(DB::FieldVisitorToString(), column_map_key[offset + p]);

                /// TODO(lta): check
                ColumnPtr new_column = makeNullable(column_map_value.cloneEmpty());
                new_column->assumeMutableRef().reserve(num_map_rows);

                iter = value_columns.try_emplace(tmp_key, new_column).first;
            }

            auto & impl_value_column = static_cast<ColumnNullable &>(iter->second->assumeMutableRef());
            /// Fill NULLs as need
            while (impl_value_column.size() < r)
                impl_value_column.insert(Null());

            /// Handle duplicated keys in map
            if (likely(impl_value_column.size() == r))
            {
                impl_value_column.getNestedColumn().insertFrom(column_map_value, offset + p);
                impl_value_column.getNullMapData().push_back(0);
            }
        }
    }

    /// Fill NULLs until all columns reach the same size
    for (auto & [k, column] : value_columns)
    {
        auto & impl_value_column = column->assumeMutableRef();
        while (impl_value_column.size() < num_map_rows)
            impl_value_column.insert(Null());
    }
}

}

void MergeTreeDataPartWriterOnDisk::Stream::finalize()
{
    compressed.next();
    /// 'compressed_buf' doesn't call next() on underlying buffer ('plain_hashing'). We should do it manually.
    plain_hashing.next();
    marks.next();

    plain_file->finalize();
    marks_file->finalize();
}

void MergeTreeDataPartWriterOnDisk::Stream::sync() const
{
    plain_file->sync();
    marks_file->sync();
}

MergeTreeDataPartWriterOnDisk::Stream::Stream(
    const String & escaped_column_name_,
    DiskPtr disk_,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    bool is_compact_map) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(disk_->writeFile(data_path_ + data_file_extension, max_compress_block_size_, is_compact_map ? WriteMode::Append: WriteMode::Rewrite)),
    plain_hashing(*plain_file),
    compressed_buf(plain_hashing, compression_codec_, max_compress_block_size_),
    compressed(compressed_buf),
    marks_file(disk_->writeFile(marks_path_ + marks_file_extension, 4096, is_compact_map ? WriteMode::Append: WriteMode::Rewrite)), marks(*marks_file),
    data_file_offset(is_compact_map ? disk_->getFileSize(data_path_ + data_file_extension): 0),
    marks_file_offset(is_compact_map ? disk_->getFileSize(marks_path_ + marks_file_extension): 0)
{
}

void MergeTreeDataPartWriterOnDisk::Stream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();
    checksums.files[name + data_file_extension].file_offset = data_file_offset;

    checksums.files[name + marks_file_extension].file_size = marks.count();
    checksums.files[name + marks_file_extension].file_hash = marks.getHash();
    checksums.files[name + marks_file_extension].file_offset = marks_file_offset;
}

void MergeTreeDataPartWriterOnDisk::Stream::deepCopyTo(Stream& target)
{
    /**
     * TODO: sanity check target stream is empty before overwritten
     */
    compressed.deepCopyTo(target.compressed);
    compressed_buf.deepCopyTo(target.compressed_buf);
    plain_hashing.deepCopyTo(target.plain_hashing);
    plain_file->deepCopyTo(*target.plain_file);

    marks.deepCopyTo(target.marks);
    marks_file->deepCopyTo(*(target.marks_file));
}

void MergeTreeDataPartWriterOnDisk::Stream::freeResource()
{
    // @EXP-insert-memory
    finalize();

    //TODO: merge freeResource code

    throw Exception("freeResource code is not merged correctly", ErrorCodes::LOGICAL_ERROR); //REMOVE IT

    //compressed.reset();
    //compressed_buf.freeResource();
    //plain_hashing.reset();
    //plain_file->freeResource();

    //marks->reset();
    //marks_file->freeResource();
}


MergeTreeDataPartWriterOnDisk::MergeTreeDataPartWriterOnDisk(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeIndices & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergeTreeDataPartWriter(data_part_,
        columns_list_, metadata_snapshot_, settings_, index_granularity_)
    , skip_indices(indices_to_recalc_)
    , part_path(data_part_->getFullRelativePath())
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , compute_granularity(index_granularity.empty())
{
    if (settings.blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);

    auto disk = data_part->volume->getDisk();
    if (!disk->exists(part_path))
        disk->createDirectories(part_path);

    for (const auto & column : columns_list)
    {
        serializations.emplace(column.name, column.type->getDefaultSerialization());
    }

    if (settings.rewrite_primary_key)
        initPrimaryIndex();
    initSkipIndices();
}

// Implementation is split into static functions for ability
/// of making unit tests without creation instance of IMergeTreeDataPartWriter,
/// which requires a lot of dependencies and access to filesystem.
static size_t computeIndexGranularityImpl(
    const Block & block,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    bool can_use_adaptive_index_granularity)
{
    size_t rows_in_block = block.rows();
    size_t index_granularity_for_block;
    if (!can_use_adaptive_index_granularity)
        index_granularity_for_block = fixed_index_granularity_rows;
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
            index_granularity_for_block = rows_in_block;
        else if (block_size_in_memory >= index_granularity_bytes)
        {
            size_t granules_in_block = block_size_in_memory / index_granularity_bytes;
            index_granularity_for_block = rows_in_block / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = block_size_in_memory / rows_in_block;
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);
    return index_granularity_for_block;
}

size_t MergeTreeDataPartWriterOnDisk::computeIndexGranularity(const Block & block) const
{
    const auto storage_settings = storage.getSettings();
    return computeIndexGranularityImpl(
            block,
            storage_settings->index_granularity_bytes,
            storage_settings->index_granularity,
            settings.blocks_are_granules_size,
            settings.can_use_adaptive_granularity);
}

void MergeTreeDataPartWriterOnDisk::initPrimaryIndex()
{
    if (metadata_snapshot->hasPrimaryKey())
    {
        index_file_stream = data_part->volume->getDisk()->writeFile(part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
    }
}

void MergeTreeDataPartWriterOnDisk::initSkipIndices()
{
    for (const auto & index_helper : skip_indices)
    {
        String stream_name = index_helper->getFileName();
        skip_indices_streams.emplace_back(
                std::make_unique<MergeTreeDataPartWriterOnDisk::Stream>(
                        stream_name,
                        data_part->volume->getDisk(),
                        part_path + stream_name, INDEX_FILE_EXTENSION,
                        part_path + stream_name, marks_file_extension,
                        default_codec, settings.max_compress_block_size));
        skip_indices_aggregators.push_back(index_helper->createIndexAggregator());
        skip_index_accumulated_marks.push_back(0);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, const Granules & granules_to_write)
{
    size_t primary_columns_num = primary_index_block.columns();
    if (index_columns.empty())
    {
        index_types = primary_index_block.getDataTypes();
        index_columns.resize(primary_columns_num);
        last_block_index_columns.resize(primary_columns_num);
        for (size_t i = 0; i < primary_columns_num; ++i)
            index_columns[i] = primary_index_block.getByPosition(i).column->cloneEmpty();
    }

    {
        /** While filling index (index_columns), disable memory tracker.
         * Because memory is allocated here (maybe in context of INSERT query),
         *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
         * And otherwise it will look like excessively growing memory consumption in context of query.
         *  (observed in long INSERT SELECTs)
         */
        MemoryTracker::BlockerInThread temporarily_disable_memory_tracker;

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (const auto & granule : granules_to_write)
        {
            if (metadata_snapshot->hasPrimaryKey() && granule.mark_on_start)
            {
                for (size_t j = 0; j < primary_columns_num; ++j)
                {
                    const auto & primary_column = primary_index_block.getByPosition(j);
                    index_columns[j]->insertFrom(*primary_column.column, granule.start_row);
                    primary_column.type->getDefaultSerialization()->serializeBinary(*primary_column.column, granule.start_row, *index_stream);
                }
            }
        }
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0; j < primary_columns_num; ++j)
        last_block_index_columns[j] = primary_index_block.getByPosition(j).column;
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write)
{
    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        for (const auto & granule : granules_to_write)
        {
            if (skip_index_accumulated_marks[i] == index_helper->index.granularity)
            {
                skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                skip_index_accumulated_marks[i] = 0;
            }

            if (skip_indices_aggregators[i]->empty() && granule.mark_on_start)
            {
                skip_indices_aggregators[i] = index_helper->createIndexAggregator();

                if (stream.compressed.offset() >= settings.min_compress_block_size)
                    stream.compressed.next();

                writeIntBinary(stream.plain_hashing.count(), stream.marks);
                writeIntBinary(stream.compressed.offset(), stream.marks);
                /// Actually this numbers is redundant, but we have to store them
                /// to be compatible with normal .mrk2 file format
                if (settings.can_use_adaptive_granularity)
                    writeIntBinary(1UL, stream.marks);
            }

            size_t pos = granule.start_row;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, granule.rows_to_write);
            if (granule.is_complete)
                ++skip_index_accumulated_marks[i];
        }
    }
}

void MergeTreeDataPartWriterOnDisk::finishPrimaryIndexSerialization(
        MergeTreeData::DataPart::Checksums & checksums, bool sync)
{
    bool write_final_mark = (with_final_mark && data_written);
    if (write_final_mark && compute_granularity)
        index_granularity.appendMark(0);

    if (index_stream)
    {
        if (write_final_mark)
        {
            for (size_t j = 0; j < index_columns.size(); ++j)
            {
                const auto & column = *last_block_index_columns[j];
                size_t last_row_number = column.size() - 1;
                index_columns[j]->insertFrom(column, last_row_number);
                index_types[j]->getDefaultSerialization()->serializeBinary(column, last_row_number, *index_stream);
            }
            last_block_index_columns.clear();
        }

        index_stream->next();
        checksums.files["primary.idx"].file_size = index_stream->count();
        checksums.files["primary.idx"].file_hash = index_stream->getHash();
        index_file_stream->finalize();
        if (sync)
            index_file_stream->sync();
        index_stream = nullptr;
    }
}

void MergeTreeDataPartWriterOnDisk::finishSkipIndicesSerialization(
        MergeTreeData::DataPart::Checksums & checksums, bool sync)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->finalize();
        stream->addToChecksums(checksums);
        if (sync)
            stream->sync();
    }

    skip_indices_streams.clear();
    skip_indices_aggregators.clear();
    skip_index_accumulated_marks.clear();
}

Names MergeTreeDataPartWriterOnDisk::getSkipIndicesColumns() const
{
    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : skip_indices)
        std::copy(index->index.column_names.cbegin(), index->index.column_names.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    return Names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());
}

void MergeTreeDataPartWriterOnDisk::addStreams(
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

void MergeTreeDataPartWriterOnDisk::addByteMapStreams(
        const NameAndTypePair & column, // implicit_name
        const String & col_name,
        const ASTPtr & effective_codec_desc)
{
    IDataType::StreamCallbackWithType callback = [&] (const ISerialization::SubstreamPath & substream_path, const IDataType & substream_type)
    {
        String stream_name = ISerialization::getFileNameForStream(column, substream_path);
        if (column_streams.count(stream_name))
            return;

        CompressionCodecPtr compression_codec;
        /// If we can use special codec then just get it
        if (ISerialization::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, &substream_type, default_codec);
        else /// otherwise return only generic codecs and don't use info about the` data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        // check map impl version
        String col_stream_name;
        if (data_part->versions->enable_compact_map_data)
        {
            col_stream_name = IDataType::getFileNameForStream(col_name, substream_path);
        }
        else
        {
            col_stream_name = stream_name;
        }

        column_streams[stream_name] = std::make_unique<Stream>(
            stream_name,
            data_part->volume->getDisk(),
            part_path + col_stream_name, DATA_FILE_EXTENSION,
            part_path + col_stream_name, marks_file_extension,
            compression_codec,
            settings.max_compress_block_size,
            data_part->versions->enable_compact_map_data);
    };

    column.type->enumerateStreams(serializations[column.name], callback);
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterOnDisk::writeUncompactedByteMapColumn(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    const Granules & granules)
{
    const auto & [name, type] = name_and_type;
    const auto & global_settings = storage.getContext()->getSettingsRef();

    // I would like to move map type serialize (expanded) logic from DataTypeMap.cpp to here because it
    // tightly bind to MergeTree storage model.

    const ColumnByteMap & column_map = typeid_cast<const ColumnByteMap &>(column);
    const auto type_map = std::dynamic_pointer_cast<const DataTypeByteMap>(type);

    String mapBaseStreamName = getBaseNameForMapCol(name);

    DataTypePtr nullValTypePtr;
    if (type_map->valueTypeIsLC())
        nullValTypePtr = type_map->getValueType();
    else
        nullValTypePtr = makeNullable(type_map->getValueType());

    /********************************************************************************************************
     * For MAP datatype expanded model, the case might be tricky here because multiple streams (e.g.
     * Merge scenario) or multiple consective blocks(large volume data) might be heterogenous. but
     * the part's bin/marks.. information should be aligned based on row sequence. How to fix it is
     * an issue here.
     * e.g. Block 1's map value is {'a':1, 'b':2}
     * This  block is serialized into parts, and generate implicit cols(aka, .bin/.mrk) looks as below:
     * __col__a, __col__b
     * Then Block 2's map value is {'a':1, 'c':3}
     * and the implicit cols __col__a, __col__b, __col__c need to be handled correctly.
     *
     * In summary,
     * While map column's data need to be serialized in storage engine, it doesn't only consider data in itself,
     * e.g. 'a', 'c' in block2, it need to integreate handled data in previous blocks. there are three cases:
     * - overlaped key, e.g. 'a', implicit column need to be appended.
     * - new key, e.g. 'c', new implicit column need to created, repaired, and appended
     * - missing key, e.g. 'b',  old implicit column need to be appended too.
     ********************************************************************************************************/

    // Look through key column to get unique keys and build corresponding implicit ColumnStream?
    // ** i.e. information in current block **

    std::set<String> & existKeyNames = existKeysNames[name];
    std::set<String> blockKeyNames;

    // Write Map implicit columns in three steps:
    // 1. fix new keys
    // 2. write overlap columns
    // 3. write missing columns

    // A bit Hack logic here as we know mapBaseStream is Nullable stream, and its two
    // substream is nested and nullmap. The stream name is generated in IDataType::getFileNameForStream,
    // we hardcode it here for simplity.
    String mapBaseStreamNameData = escapeForFileName(mapBaseStreamName);
    // check whether this is new Key
    bool mapBaseStreamExist = column_streams.count(mapBaseStreamNameData);

    /// 1. It's faster to use StringRef as key of unordered_map than Field if type of map key is String
    /// 2. And it would be better to use integer when the type of map key belongs to integer class,
    ///    but, which is a rare case in current situation. It's easy to optimize this case but more
    ///    messy code would be added. DO IT when someone really need it.
    std::unordered_map<StringRef, String> key_name_map;
    std::unordered_map<StringRef, ColumnPtr> value_columns;

    /// TODO(lta): check and support
    if (optimize_map_column_serialization)
    {
        constructImplicitValueColumns(column_map, key_name_map, value_columns);
    }
    else
    {
        auto & column_map_key = column_map.getKey();
        for (size_t i = 0; i < column_map_key.size(); ++i)
        {
            auto tmp_key = column_map_key.getDataAt(i);
            if (key_name_map.find(tmp_key) == key_name_map.end())
                key_name_map.try_emplace(tmp_key, applyVisitor(DB::FieldVisitorToString(), column_map_key[i]));
        }
    }

    //TODO: patch MAP KEY # check feature idependently
    // checkMapKey(name, key_name_map);
    ISerialization::SerializeBinaryBulkSettings serialize_settings;

	auto nullValSerial = nullValTypePtr->getDefaultSerialization();

    // We should construct or build WriteBuffer(ColumnStream) based on unique keys
    for (auto & k_n : key_name_map)
    {
        String implicitStreamName = getImplicitColNameForMapKey(name, k_n.second);

        if (escapeForFileName(implicitStreamName).size() > DBMS_MAX_FILE_NAME_LENGTH)
        {
            LOG_WARNING(getLogger(), "The file name of map key is too long, more than {}, discard key: {}",
					    DBMS_MAX_FILE_NAME_LENGTH, k_n.second);
            continue;
        }

        blockKeyNames.insert(k_n.second);
        bool needFixNewKey = (!existKeyNames.count(k_n.second) && mapBaseStreamExist);

    	auto [it, inserted] = serialization_states.emplace(implicitStreamName, nullptr);
		serializations.emplace(implicitStreamName, nullValSerial);

        NameAndTypePair implicit_column{implicitStreamName, nullValTypePtr};
        if (!existKeyNames.count(k_n.second))
            implicit_columns_list.emplace_back(implicit_column);

        if (needFixNewKey)
        {
            this->deepCopyAndAdd(mapBaseStreamName, implicitStreamName, *nullValTypePtr);
        }
        else
        {
            this->addStreams(implicit_column, default_codec->getFullCodecDesc());
        }


        ColumnPtr implicitValueCol;
        if (optimize_map_column_serialization)
            implicitValueCol = value_columns[k_n.first];
        else
            implicitValueCol = column_map.getValueColumnByKey(k_n.first);

        // Invoke writeColumn for those generated implicit value column
        if (type_map->valueTypeIsLC())
        {
            serialize_settings.getter = createStreamGetter(implicit_column,  offset_columns);
            serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
            serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;
            nullValSerial->serializeBinaryBulkStatePrefix(serialize_settings, it->second);
        }

        this->writeColumn(implicit_column, *implicitValueCol, offset_columns, granules);
    }

    // construct a fake column, could be optimized here.
    // Note cloneResized(size) could cause trouble as there was bug in ColumnVector::cloneResize,
    // Dirty data might exist in data container, even we mark this row as NULL, write dirty data
    // into part could cause checksum mismatch among replicas even the data is ok for use.
    // Part merge logic will check checksum among replicas and report ERROR in log. this will
    // block merge process somehow in case checksum mismatch happens.
    //
    //auto fakeCol = makeNullable(column_map.getValuePtr()->cloneResized(column_map.size()));
    //const_cast<ColumnNullable&>(typeid_cast<const ColumnNullable&>(*fakeCol)).makeAllNulls();

    ColumnPtr fakeCol;
    if (type_map->valueTypeIsLC())
        fakeCol = column_map.getValuePtr()->cloneEmpty();
    else
        fakeCol = makeNullable(column_map.getValuePtr()->cloneEmpty());

    fakeCol = fakeCol->cloneResized(column_map.size());

    // Fix missing columns
    for (auto & ek : existKeyNames)
    {
        if (!blockKeyNames.count(ek))
        {
            String  streamName = getImplicitColNameForMapKey(name, ek);

    		auto [it2, inserted2] = serialization_states.emplace(streamName, nullptr);
			serializations.emplace(streamName, nullValSerial);

            NameAndTypePair implicit_column{streamName, nullValTypePtr};
            this->addStreams(implicit_column, default_codec->getFullCodecDesc());

            if (type_map->valueTypeIsLC())
            {
                serialize_settings.getter = createStreamGetter(implicit_column, offset_columns);
                nullValSerial->serializeBinaryBulkStatePrefix(serialize_settings, it2->second);
            }

            this->writeColumn(implicit_column, *fakeCol, offset_columns, granules);

        }
    }

    // append info into map base implicit column
    auto [it3, inserted3] = serialization_states.emplace(mapBaseStreamName, nullptr);
    serializations.emplace(mapBaseStreamName, nullValSerial);

    NameAndTypePair base_column{mapBaseStreamName, nullValTypePtr};
    if (!mapBaseStreamExist)
        implicit_columns_list.emplace_back(base_column);

    this->addStreams(base_column, default_codec->getFullCodecDesc());
    if (type_map->valueTypeIsLC())
    {
        serialize_settings.getter = createStreamGetter(base_column, offset_columns);
        nullValSerial->serializeBinaryBulkStatePrefix(serialize_settings, it3->second);
    }

    this->writeColumn(base_column, *fakeCol, offset_columns, granules);

    // after write this map column, update exist keys for next block check
    existKeyNames.insert(blockKeyNames.begin(), blockKeyNames.end());
}

void MergeTreeDataPartWriterOnDisk::writeCompactedByteMapColumn(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    const Granules & granules)
{
	 /********************************************************************************************************
     * For MAP datatype expanded model, the case might be tricky here because multiple streams (e.g.
     * Merge scenario) or multiple consecutive blocks(large volume data) might be heterogenous. but
     * the part's bin/marks.. information should be aligned based on row sequence. How to fix it is
     * an issue here.
     * In order to avoid too many small files, all implicit columns of map type data are stored in the same file.
     * Therefore, offset needs to be added to the checksum to indicate the offset of the implicit column data
     * in the file.
     *
     * Here is a Map column format example:
     * Map column t has two implicit keys: a and b. They both have two blocks and the second block is not full.
     *
     *         ________    ___                      ________    ___
     *        |        |                           | mark1  |    ↑
     *        | Block1 |    ↑                      |________|    a
     *        |________|    a                      | mark2  |    ↓
     *        | Block2 |    ↓                      |________|   ___
     *        |________|   ___                     | mark1  |    ↑
     *        |        |                           |________|    b
     *        | Block1 |    ↑                      | mark2  |    ↓
     *        |________|    b                      |________|   ___
     *        | Block2 |    ↓
     *        |________|   ___
     *          t.bin                                t.mrk
     *
     * Therefore, it's necessary to record the file_offset of the implcit column.
     *
     * In summary,
     * While map column's data need to be serialized in storage engine, it only considers data in itself.
     *
     ********************************************************************************************************/
    const auto & [name, type] = name_and_type;
    if (!type->isMap())
    {
        throw Exception("Data whose type is not map is processed in method `writeCompactedByteMapColumn`", ErrorCodes::LOGICAL_ERROR);
    }

    const ColumnByteMap & column_map = typeid_cast<const ColumnByteMap &>(column);
    const auto type_map = std::dynamic_pointer_cast<const DataTypeByteMap>(type);

    // NOTE: for business reason, LC is considered not very useful
    DataTypePtr nullValTypePtr = makeNullable(type_map->getValueType());

    /// 1. It's faster to use StringRef as key of unordered_map than Field if type of map key is String
    /// 2. And it would be better to use integer when the type of map key belongs to integer class,
    ///    but, which is a rare case in current situation. It's easy to optimize this case but more
    ///    messy code would be added. DO IT when someone really need it.
    std::unordered_map<StringRef, String> key_name_map;
    std::unordered_map<StringRef, ColumnPtr> value_columns;

    if (optimize_map_column_serialization)
    {
        constructImplicitValueColumns(column_map, key_name_map, value_columns);
    }
    else
    {
        auto & column_map_key = column_map.getKey();
        for (size_t i = 0; i < column_map_key.size(); ++i)
        {
            auto tmp_key = column_map_key.getDataAt(i);
            if (key_name_map.find(tmp_key) == key_name_map.end())
                key_name_map.try_emplace(tmp_key, applyVisitor(DB::FieldVisitorToString(), column_map_key[i]));
        }
    }

    //TODO: patch MAP KEY # check feature idependently
    // checkMapKey(name, key_name_map);
    ISerialization::SerializeBinaryBulkSettings serialize_settings;

	auto nullValSerial = nullValTypePtr->getDefaultSerialization();

    for (auto & k_n : key_name_map)
    {
        String implicitStreamName = getImplicitColNameForMapKey(name, k_n.second);

        if (escapeForFileName(implicitStreamName).size() > DBMS_MAX_FILE_NAME_LENGTH)
        {
            LOG_WARNING(getLogger(), "The file name of map key is too long, more than {}, discard key: {}",
                        DBMS_MAX_FILE_NAME_LENGTH, k_n.second);
            continue;
        }

        // Fill up implicit column info in auxilary structures
        serialization_states.emplace(implicitStreamName, nullptr);
		serializations.emplace(implicitStreamName, nullValSerial);

        // All implicit column data store in the same file
        this->addByteMapStreams({implicitStreamName, nullValTypePtr}, name, default_codec->getFullCodecDesc());

        ColumnPtr implicitValueCol;
        if (optimize_map_column_serialization)
            implicitValueCol = value_columns[k_n.first];
        else
            implicitValueCol = column_map.getValueColumnByKey(k_n.first);

        // Invoke writeData for those generated implicit value column
        this->writeColumn(
            {implicitStreamName, nullValTypePtr},
            *implicitValueCol,
            offset_columns,
            granules,
            !is_merge); // Since all implicit column data store in the same file, after all blocks of the same implicit column data are written, it is necessary to release resources and persist the data to disk. Otherwise, the offset is incorrect.
                        // But when it's in merge status, it uses vertical merge algorithm and will close the writer after each implicit column finishes, so it's no need to finalize stream. Because the granules is not fixed, so finalizing stream will get different granules int the same part.
    }
}

ISerialization::StreamCallback MergeTreeDataPartWriterOnDisk::finalizeStreams(const String & name)
{
    return [&](const ISerialization::SubstreamPath & sub_stream_path) -> void {
        String stream_name = ISerialization::getFileNameForStream(name, sub_stream_path);

        auto & stream = *column_streams[stream_name];
        stream.finalize();
    };
}

void MergeTreeDataPartWriterOnDisk::deepCopyAndAdd(
	const String & sourceName,
	const String & targetName,
	const IDataType & type)
{
    // This is done in three steps:
    // 1. copy synced data in file
    // 2. copy data in (layed) buffers and status in the buffers
    try
    {
        type.enumerateStreams(
			type.getDefaultSerialization(),
            [&](const ISerialization::SubstreamPath& substream_path, const IDataType&)
            {
                auto sourceStreamName = IDataType::getFileNameForStream(sourceName, substream_path);
                auto targetStreamName = IDataType::getFileNameForStream(targetName, substream_path);
                if (!column_streams.count(sourceStreamName) ||
                    column_streams.count(targetStreamName))
                {
                    throw Exception("prerequist is not matched while calling IMergedBlockOutputStream::deepCopyAndAdd", ErrorCodes::LOGICAL_ERROR);
                }

                column_streams[sourceStreamName]->sync();

                // copy flushed files
                if (Poco::File(part_path + sourceStreamName + DATA_FILE_EXTENSION).exists())
                    Poco::File(part_path + sourceStreamName + DATA_FILE_EXTENSION).copyTo(
                            part_path + targetStreamName + DATA_FILE_EXTENSION);
                if (Poco::File(part_path + sourceStreamName + marks_file_extension).exists())
                    Poco::File(part_path + sourceStreamName + marks_file_extension).copyTo(
                            part_path + targetStreamName + marks_file_extension);

                // addStreams
                column_streams[targetStreamName] = std::make_unique<Stream>(
                        targetStreamName,
						data_part->volume->getDisk(),
                        part_path + targetStreamName, DATA_FILE_EXTENSION,
                        part_path + targetStreamName, marks_file_extension,
                        default_codec,
                        settings.max_compress_block_size);

                // copy buffered stream and its info
                column_streams[sourceStreamName]->deepCopyTo(*column_streams[targetStreamName]);
            }, {});
    }
    catch(Exception& e)
    {
        e.addMessage("MergeTreeDataPartWriterWide::deepCopyAndAdd fail");
        throw;
    }
}

ISerialization::OutputStreamGetter MergeTreeDataPartWriterOnDisk::createStreamGetter(
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

void MergeTreeDataPartWriterOnDisk::writeSingleGranule(
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

StreamsWithMarks MergeTreeDataPartWriterOnDisk::getCurrentMarksForColumn(
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

StreamsWithMarks MergeTreeDataPartWriterOnDisk::copyLastNonWrittenMarks(
    const NameAndTypePair & source_column,
    const StreamsWithMarks & source_marks,
    const NameAndTypePair & target_column,
    WrittenOffsetColumns & offset_columns,
    ISerialization::SubstreamPath & path)
{
    std::map<String, StreamNameAndMark> source_mark_map;
    for (const auto & mark: source_marks)
        source_mark_map[mark.stream_name] = mark;
    StreamsWithMarks result;
    serializations[target_column.name]->enumerateStreams(
        [&](const ISerialization::SubstreamPath & substream_path) {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;

            String target_stream_name = ISerialization::getFileNameForStream(target_column, substream_path);

            /// Don't write offsets more than one time for Nested type.
            if (is_offsets && offset_columns.count(target_stream_name))
                return;

            String source_stream_name = ISerialization::getFileNameForStream(source_column, substream_path);
            if (!source_mark_map.count(source_stream_name))
                throw Exception(
                    "Mark info of source stream name " + source_stream_name
                        + " is not exist. Target source stream name: " + target_stream_name,
                    ErrorCodes::LOGICAL_ERROR);

            StreamNameAndMark stream_with_mark = source_mark_map[source_stream_name];
            stream_with_mark.stream_name = target_stream_name;
            result.push_back(stream_with_mark);
        },
        path);

    return result;
}

void MergeTreeDataPartWriterOnDisk::writeSingleMark(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    size_t number_of_rows,
    ISerialization::SubstreamPath & path)
{
    StreamsWithMarks marks = getCurrentMarksForColumn(column, offset_columns, path);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterOnDisk::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    Stream & stream = *column_streams[stream_with_mark.stream_name];
    writeIntBinary(stream_with_mark.mark.offset_in_compressed_file, stream.marks);
    writeIntBinary(stream_with_mark.mark.offset_in_decompressed_block, stream.marks);
    if (settings.can_use_adaptive_granularity)
        writeIntBinary(rows_in_mark, stream.marks);
}

void MergeTreeDataPartWriterOnDisk::writeFinalMark(
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

}
