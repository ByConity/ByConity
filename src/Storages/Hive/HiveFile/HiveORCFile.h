#pragma once

#include "Common/config.h"
#include "Processors/Formats/Impl/ArrowColumnToCHColumn.h"
#if USE_HIVE

#include "Storages/Hive/HiveFile/IHiveFile.h"

namespace arrow::adapters::orc { class ORCFileReader; }
namespace orc { class Statistics; }

namespace DB
{
class HiveORCFile : public IHiveFile
{
public:
    Features getFeatures() const override
    {
        return Features{
            .support_file_splits = true,
            .support_file_minmax_index = false,
            .support_split_minmax_index = false,
        };
    }

    HiveORCFile();
    ~HiveORCFile() override;

    size_t numSlices() const override;
    std::optional<size_t> numRows() const override;
    SourcePtr getReader(const Block & block, const std::shared_ptr<IHiveFile::ReadParams> & params) override;

private:
    MinMaxIndexPtr buildMinMaxIndex(const orc::Statistics * statistics, const NamesAndTypesList & index_names_and_types) const;
    /// void loadFileMinMaxIndex(const NamesAndTypesList & index_names_and_types) override;
    void loadSplitMinMaxIndex(const NamesAndTypesList & index_names_and_types) override;

    void openFile() const;
    mutable std::map<String, size_t> orc_column_positions;
    mutable std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;
    mutable std::unique_ptr<ReadBuffer> buf;
    mutable std::shared_ptr<arrow::Schema> schema;
    mutable std::mutex mutex;
};

class ArrowColumnToCHColumn;

class ORCSliceSource : public ISource
{
public:
    ORCSliceSource(
        std::unique_ptr<ReadBuffer> in_,
        std::shared_ptr<arrow::adapters::orc::ORCFileReader> reader_,
        std::vector<int> column_indices_,
        std::shared_ptr<IHiveFile::ReadParams> read_params_,
        std::shared_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column_);

    ~ORCSliceSource() override;
    String getName() const override { return "ORCSliceSource"; }
    Chunk generate() override;

private:
    std::unique_ptr<ReadBuffer> in;
    std::shared_ptr<arrow::adapters::orc::ORCFileReader> reader;
    std::vector<int> column_indices;
    std::shared_ptr<IHiveFile::ReadParams> read_params;
    std::shared_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
};

}

#endif
