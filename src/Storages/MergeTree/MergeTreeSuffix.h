#pragma once
#include <Core/Types.h>

namespace DB
{
constexpr auto DATA_FILE_EXTENSION = ".bin";
constexpr auto INDEX_FILE_EXTENSION = ".idx";

constexpr auto BLOOM_FILTER_FILE_EXTENSION = ".bmf";
constexpr auto RANGE_BLOOM_FILTER_FILE_EXTENSION = ".rbmf";

constexpr auto COMPRESSION_COLUMN_EXTENSION = "_encoded";
constexpr auto COMPRESSION_DATA_FILE_EXTENSION = "_encoded.bin";
constexpr auto COMPRESSION_MARKS_FILE_EXTENSION = "_encoded.mrk";

constexpr auto AB_IDX_EXTENSION = ".adx";
constexpr auto AB_IRK_EXTENSION = ".ark";

constexpr auto MARK_BITMAP_IDX_EXTENSION = ".m_adx";
constexpr auto MARK_BITMAP_IRK_EXTENSION = ".m_ark";

constexpr auto BITENGINE_COLUMN_EXTENSION = "_encoded_bitmap";
constexpr auto BITENGINE_DATA_FILE_EXTENSION = "_encoded_bitmap.bin";
constexpr auto BITENGINE_DATA_MARKS_EXTENSION = "_encoded_bitmap.mrk2";

constexpr auto DELETE_BITMAP_FILE_EXTENSION = ".del";
constexpr auto DELETE_BITMAP_FILE_NAME = "delete.del";

constexpr auto KLL_FILE_EXTENSION = ".kll";

constexpr auto UKI_FILE_NAME = "unique_key.idx";   /// name of unique key index file

bool isEngineReservedWord(const String & column);

/// Type `ONLY_SOURCE` is used in encoding and select, it's **DEFAULT** type
/// Type `ONLY_ENCODE` used in BitEngine parts merge
/// Type `BOTH` is used in BitEngineDictionaryManager::checkEncodedPart
enum BitEngineReadType { ONLY_SOURCE, ONLY_ENCODE, BOTH, };
}
