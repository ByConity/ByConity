#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int INCORRECT_DATA;
}

/// Base class for schema inference for the data in some specific format.
/// It reads some data from read buffer and tries to determine the schema
/// from read data.
class ISchemaReader
{
public:
    explicit ISchemaReader(ReadBuffer & in_) : in(in_) {}

    virtual NamesAndTypesList readSchema() = 0;

    /// Some formats like Parquet contains number of rows in metadata
    /// and we can read it once during schema inference and reuse it later for fast count;
    virtual std::optional<size_t> readNumberOrRows() { return std::nullopt; }

    /// True if order of columns is important in format.
    /// Exceptions: JSON, TSKV.
    virtual bool hasStrictOrderOfColumns() const { return true; }

    virtual bool needContext() const { return false; }
    virtual void setContext(const ContextPtr &) {}

    virtual void setMaxRowsToRead(size_t) {}
    virtual void setMaxRowsAndBytesToRead(size_t, size_t) {}
    virtual size_t getNumRowsRead() const { return 0; }

    // virtual void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type);
    // virtual void transformTypesFromDifferentFilesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) { transformTypesIfNeeded(type, new_type); }

    virtual ~ISchemaReader() = default;

protected:
    ReadBuffer & in;
};


}
