#pragma once

#include <common/defines.h>
#include <Common/PODArray_fwd.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// vec_entry_per_value means how many vector entry is mapping to one row, since some
/// data type like FixString may contains multiple vector entry
template <typename VecValueType>
size_t deserializeBinaryBulkForVector(PaddedPODArray<VecValueType>& container,
    ReadBuffer& istr, size_t limit, const UInt8* filter, const size_t vec_entry_per_val)
{
    const size_t size_per_val = sizeof(VecValueType) * vec_entry_per_val;
    size_t initial_size = container.size();
    container.resize(initial_size + limit * vec_entry_per_val);

    size_t processed_bytes = 0;
    size_t result_bytes = 0;
    char* current_data_pos = reinterpret_cast<char*>(&container[initial_size]);
    if (filter == nullptr)
    {
        processed_bytes = istr.readBig(current_data_pos, size_per_val * limit);
        result_bytes = processed_bytes;
    }
    else
    {
        static constexpr size_t SIMD_BYTES = 64;
        const size_t bytes_per_simd = size_per_val * SIMD_BYTES;

        const UInt8* filter_pos = filter;
        const UInt8* filter_end = filter + limit;
        const UInt8* aligned_filter_end = filter + limit / SIMD_BYTES * SIMD_BYTES;

        for (; !istr.eof() && filter_pos < aligned_filter_end; filter_pos += SIMD_BYTES)
        {
            UInt64 mask = bytes64MaskToBits64Mask(filter_pos);

            if (0xffffffffffffffff == mask)
            {
                size_t readed = istr.read(current_data_pos, bytes_per_simd);
                current_data_pos += readed;
                processed_bytes += readed;
                result_bytes += readed;
            }
            else if (0x0 == mask)
            {
                size_t ignored = istr.tryIgnore(size_per_val * SIMD_BYTES);
                processed_bytes += ignored;
            }
            else
            {
                size_t processed_rows_in_batch = 0;

                while (mask)
                {
                    size_t index = __builtin_ctzll(mask);

                    if (index > processed_rows_in_batch)
                    {
                        size_t ignored = istr.tryIgnore((index - processed_rows_in_batch) * size_per_val);
                        processed_bytes += ignored;
                    }

                    size_t readed = istr.read(current_data_pos, size_per_val);
                    current_data_pos += readed;
                    processed_bytes += readed;
                    result_bytes += readed;
                    processed_rows_in_batch += index - processed_rows_in_batch + 1;

#ifdef __BMI__
                    mask = _blsr_u64(mask);
#else
                    mask = mask & (mask-1);
#endif
                }

                if (processed_rows_in_batch < SIMD_BYTES)
                {
                    size_t ignored = istr.tryIgnore((SIMD_BYTES - processed_rows_in_batch) * size_per_val);
                    processed_bytes += ignored;
                }
            }
        }

        for (; !istr.eof() && filter_pos < filter_end; ++filter_pos)
        {
            if (*filter_pos != 0)
            {
                size_t readed = istr.read(current_data_pos, size_per_val);
                current_data_pos += readed;
                processed_bytes += readed;
                result_bytes += readed;
            }
            else
            {
                size_t ignored = istr.tryIgnore(size_per_val);
                processed_bytes += ignored;
            }
        }
    }

    if (unlikely(processed_bytes % size_per_val != 0 || result_bytes % size_per_val != 0))
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data. Bytes "
            "processed bytes: {}, result bytes: {}, size per object {}", processed_bytes,
            result_bytes, size_per_val);
    }

    container.resize(initial_size + (result_bytes / size_per_val) * vec_entry_per_val);
    return processed_bytes / size_per_val;
}

}
