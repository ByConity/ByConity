#pragma once
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

namespace DB
{

/// Writes data part in compact format.
class MergeTreeDataPartWriterCompact : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterCompact(
        const MergeTreeData::DataPartPtr & data_part,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) override;

private:
    /// Finish serialization of the data. Flush rows in buffer to disk, compute checksums.
    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    /// Write block of rows into .bin file and marks in .mrk files
    void writeDataBlock(const Block & block, const Granules & granules);

    /// all implicit column is written into different file against other columns who will be written into one file.
    void writeAllImplicitColumnBlock(const Block & block, const Granules & granules);

    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    void writeColumn(
        const NameAndTypePair & name_and_type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        const Granules & granules,
        bool need_finalize = false) override;

    /// Write block of rows into .bin file and marks in .mrk files, primary index in .idx file
    /// and skip indices in their corresponding files.
    void writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules);
    
    Poco::Logger * getLogger() override { return log; } 

    Block header;

    /** Simplified SquashingTransform. The original one isn't suitable in this case
      *  as it can return smaller block from buffer without merging it with larger block if last is enough size.
      * But in compact parts we should guarantee, that written block is larger or equals than index_granularity.
      */
    class ColumnsBuffer
    {
    public:
        void add(MutableColumns && columns);
        size_t size() const;
        Columns releaseColumns();
    private:
        MutableColumns accumulated_columns;
    };

    ColumnsBuffer columns_buffer;

    /// Compressed stream which allows to write with codec.
    struct CompressedStream
    {
        CompressedWriteBuffer compressed_buf;
        HashingWriteBuffer hashing_buf;

        CompressedStream(WriteBuffer & buf, const CompressionCodecPtr & codec)
            : compressed_buf(buf, codec)
            , hashing_buf(compressed_buf) {}
    };
    using CompressedStreamPtr = std::shared_ptr<CompressedStream>;
    
    struct CompactDataWriter
    {
        CompactDataWriter(
            const DiskPtr & disk,
            const String & part_path,
            const String & marks_file_extension,
            const MergeTreeWriterSettings & settings,
            const CompressionCodecPtr default_codec);

        void finalize();

        void sync() const;

        void addToChecksums(IMergeTreeDataPart::Checksums & checksums);

        void addDataStreams(const NameAndTypePair & column, const ASTPtr & effective_codec_desc, SerializationsMap & serializations);

        const CompressionCodecPtr default_codec;

        String marks_file_extension;

        /// hashing_buf -> compressed_buf -> plain_hashing -> plain_file
        std::unique_ptr<WriteBufferFromFileBase> plain_file;
        HashingWriteBuffer plain_hashing;

        /// marks -> marks_file
        std::unique_ptr<WriteBufferFromFileBase> marks_file;
        HashingWriteBuffer marks;

        /// Create compressed stream for every different codec. All streams write to
        /// a single file on disk.
        std::unordered_map<UInt64, CompressedStreamPtr> streams_by_codec;

        /// Stream for each column's substreams path (look at addStreams).
        std::unordered_map<String, CompressedStreamPtr> compressed_streams;

    };

    using CompactDataWriterPtr = std::unique_ptr<CompactDataWriter>;
    /// add comment(lta)
    CompactDataWriterPtr data_writer;

    Poco::Logger * log;

};

}
