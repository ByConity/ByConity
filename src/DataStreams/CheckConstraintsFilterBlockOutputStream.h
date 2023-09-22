#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/ConstraintsDescription.h>
#include <Interpreters/StorageID.h>


namespace DB
{

/** Check for constraints violation. If anything is found - throw an exception with detailed error message.
  * Otherwise just pass block to output unchanged.
  */

class CheckConstraintsFilterBlockOutputStream : public IBlockOutputStream
{
public:
    CheckConstraintsFilterBlockOutputStream(
            const StorageID & table_,
            const BlockOutputStreamPtr & output_,
            const Block & header_,
            const ConstraintsDescription & constraints_,
            ContextPtr context_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

    const Block & getWrittenBlock() const { return written_block; }

private:
    StorageID table_id;
    BlockOutputStreamPtr output;
    Block header;
    Block written_block;
    const ConstraintsDescription constraints;
    const ConstraintsExpressions expressions;
};
}
