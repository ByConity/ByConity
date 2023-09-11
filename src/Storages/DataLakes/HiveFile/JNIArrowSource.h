#pragma once
#include "Common/config.h"
#if USE_JAVA_EXTENSIONS

#include "Processors/ISource.h"
#include "Formats/FormatSettings.h"

namespace arrow { class Schema; }

namespace DB
{
class ArrowColumnToCHColumn;
class JNIArrowReader;

class JNIArrowSource : public ISource
{
public:
    JNIArrowSource(
        Block header_,
        std::unique_ptr<JNIArrowReader> reader_);

    ~JNIArrowSource() override;

    Chunk generate() override;
    String getName() const override { return "JNI"; }

private:
    void prepareReader();

    bool initialized = false;
    std::shared_ptr<arrow::Schema> schema;
    std::unique_ptr<JNIArrowReader> reader;
    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
};
}
#endif
