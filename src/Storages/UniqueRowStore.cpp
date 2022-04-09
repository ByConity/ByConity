#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/UniqueRowStore.h>
#include <Common/Coding.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_FORMAT;
}

UniqueRowStore::UniqueRowStore(const String & file_path, IndexFileBlockCachePtr block_cache)
{
    IndexFile::Options options;
    options.block_cache = std::move(block_cache);
    auto local_reader = std::make_unique<IndexFile::IndexFileReader>(options);
    auto status = local_reader->Open(file_path);
    if (!status.ok())
        throw Exception("Failed to open index file " + file_path + ": " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    index_reader = std::move(local_reader);
}

std::unique_ptr<IndexFile::Iterator> UniqueRowStore::new_iterator(const IndexFile::ReadOptions & options)
{
    if (!index_reader)
        return std::unique_ptr<IndexFile::Iterator>(IndexFile::NewEmptyIterator());
    std::unique_ptr<IndexFile::Iterator> res;
    auto st = index_reader->NewIterator(options, &res);
    if (!st.ok())
        throw Exception("Failed to get iterator: " + st.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    return res;
}

size_t UniqueRowStore::residentMemoryUsage() const
{
    return index_reader ? index_reader->ResidentMemoryUsage() : sizeof(UniqueRowStore);
}

void UniqueRowStoreMeta::write(WriteBuffer & to)
{
    writeString("unique row store format version: 1\n", to);

    writeVarUInt(columns.size(), to);
    for (const auto & name_and_type : columns)
        name_and_type.serialize(to);

    writeVarInt(removed_columns.size(), to);
    for (const auto & name: removed_columns)
        writeBinary(name, to);
}

void UniqueRowStoreMeta::read(ReadBuffer & in)
{
    assertString("unique row store format version: ", in);
    size_t format_version;
    readText(format_version, in);
    assertChar('\n', in);
    if (format_version != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Bad unique row store format version {}", format_version);

    size_t size;
    readVarUInt(size, in);
    for (size_t i = 0; i < size; ++i)
    {
        NameAndTypePair elem;
        elem.deserialize(in);
        columns.emplace_back(std::move(elem));
    }

    readVarUInt(size, in);
    for (size_t i = 0; i < size; ++i)
    {
        String name;
        readBinary(name, in);
        removed_columns.emplace(name);
    }
}

}
