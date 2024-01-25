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

namespace DB
{

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType
{
public:
    static constexpr bool is_parametric = true;

    explicit DataTypeNullable(const DataTypePtr & nested_data_type_);
    std::string doGetName() const override { return "Nullable(" + nested_data_type->getName() + ")"; }
    const char * getFamilyName() const override { return "Nullable"; }
    TypeIndex getTypeId() const override { return TypeIndex::Nullable; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested_data_type->cannotBeStoredInTables(); }
    bool shouldAlignRightInPrettyFormats() const override { return nested_data_type->shouldAlignRightInPrettyFormats(); }
    bool textCanContainOnlyValidUTF8() const override { return nested_data_type->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested_data_type->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested_data_type->canBeComparedWithCollation(); }
    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return nested_data_type->isSummable(); }
    bool canBeUsedInBooleanContext() const override { return nested_data_type->canBeUsedInBooleanContext() || onlyNull(); }
    bool haveMaximumSizeOfValue() const override { return nested_data_type->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return 1 + nested_data_type->getMaximumSizeOfValueInMemory(); }
    bool isNullable() const override { return true; }
    size_t getSizeOfValueInMemory() const override;
    bool onlyNull() const override;
    bool canBeInsideLowCardinality() const override { return nested_data_type->canBeInsideLowCardinality(); }

    /// DataTypeNullable cannot be ByteMap value type, but we need to compatible old invalid tables,
    /// this will be checked in MergeTreeMetaBase::checkColumnsValidity for newly created tables
    bool canBeByteMapValueType() const override { return nested_data_type->canBeByteMapValueType(); }

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const IColumn & column) const override;
    // SerializationPtr getSubcolumnSerialization(
    //     const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const override;

    const DataTypePtr & getNestedType() const { return nested_data_type; }

private:
    SerializationPtr doGetDefaultSerialization() const override;

    DataTypePtr nested_data_type;
};


DataTypePtr makeNullable(const DataTypePtr & type);
DataTypePtr removeNullable(const DataTypePtr & type);
DataTypePtr makeNullableOrLowCardinalityNullable(const DataTypePtr & type);

}
