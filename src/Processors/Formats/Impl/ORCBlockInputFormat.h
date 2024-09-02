#pragma once
#include "config_formats.h"
#if USE_ORC

#    include <Formats/FormatSettings.h>
#    include <Processors/Formats/IInputFormat.h>

namespace arrow::adapters::orc
{
class ORCFileReader;
}
namespace arrow
{
class Schema;
}

namespace DB
{
class ArrowColumnToCHColumn;
class ORCBlockInputFormat : public IInputFormat
{
public:
    ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

    void setQueryInfo(const SelectQueryInfo &, ContextPtr) override;

    IStorage::ColumnSizeByName getColumnSizes() override;

    const BlockMissingValues & getMissingValues() const override;

    static std::vector<int> getColumnIndices(const std::shared_ptr<arrow::Schema> & schema, const Block & header, const bool & ignore_case, const bool & import_nested);
protected:
    Chunk generate() override;

private:
    // TODO: check that this class implements every part of its parent

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    int stripe_total = 0;

    int stripe_current = 0;

    // indices of columns to read from ORC file
    std::vector<int> include_indices;

    const FormatSettings format_settings;

    BlockMissingValues block_missing_values;

    void prepareReader();
};

}
#endif
