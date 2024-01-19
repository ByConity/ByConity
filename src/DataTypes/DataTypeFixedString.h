/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <DataTypes/IDataType.h>
#include <Common/PODArray_fwd.h>
#include <Common/Exception.h>

#define MAX_FIXEDSTRING_SIZE 0xFFFFFF
#define MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS 256


namespace DB
{
class ColumnFixedString;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}


class DataTypeFixedString final : public IDataType
{
private:
    size_t n;

public:
    using ColumnType = ColumnFixedString;

    static constexpr bool is_parametric = true;
    static constexpr auto type_id = TypeIndex::FixedString;

    explicit DataTypeFixedString(size_t n_) : n(n_)
    {
        if (n == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "FixedString size must be positive");
        if (n > MAX_FIXEDSTRING_SIZE)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "FixedString size is too large");
    }

    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return type_id; }

    const char * getFamilyName() const override { return "FixedString"; }

    size_t getN() const
    {
        return n;
    }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    SerializationPtr doGetDefaultSerialization() const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return n; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeMapKeyType() const override { return true; }
    bool canBeMapValueType() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }
    Field stringToVisitorField(const String& ins) const override;
    String stringToVisitorString(const String & ins) const override;

    /// Makes sure that the length of a newly inserted string to `chars` is equal to getN().
    /// If the length is less than getN() the function will add zero characters up to getN().
    /// If the length is greater than getN() the function will throw an exception.
    void alignStringLength(PaddedPODArray<UInt8> & chars, size_t old_size) const;
};

}
