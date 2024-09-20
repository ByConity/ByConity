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

#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Core/NamesAndTypes.h>
#include <Common/checkStackSize.h>
#include <Common/RowExistsColumnInfo.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/ColumnsDescription.h>
#include <Columns/ColumnConst.h>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace
{

/// Columns absent in part may depend on other absent columns so we are
/// searching all required physical columns recursively. Return true if found at
/// least one existing (physical) column in part.
bool injectRequiredColumnsRecursively(
    const String & column_name,
    const ColumnsDescription & storage_columns,
    const MergeTreeMetaBase::AlterConversions & alter_conversions,
    const MergeTreeMetaBase::DataPartPtr & part,
    Names & columns,
    NameSet & required_columns,
    NameSet & injected_columns)
{
    /// This is needed to prevent stack overflow in case of cyclic defaults or
    /// huge AST which for some reason was not validated on parsing/interpreter
    /// stages.
    checkStackSize();

    auto column_in_storage = storage_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
    if (column_in_storage)
    {
        auto column_name_in_part = column_in_storage->getNameInStorage();
        if (alter_conversions.isColumnRenamed(column_name_in_part))
            column_name_in_part = alter_conversions.getColumnOldName(column_name_in_part);

        auto column_in_part = NameAndTypePair(
            column_name_in_part, column_in_storage->getSubcolumnName(),
            column_in_storage->getTypeInStorage(), column_in_storage->type);

        /// column has files and hence does not require evaluation
        if (part->hasColumnFiles(column_in_part))
        {
            /// ensure each column is added only once
            if (required_columns.count(column_name) == 0)
            {
                columns.emplace_back(column_name);
                required_columns.emplace(column_name);
                injected_columns.emplace(column_name);
            }
            return true;
        }
    }

    /// Column doesn't have default value and don't exist in part
    /// don't need to add to required set.
    const auto column_default = storage_columns.getDefault(column_name);
    if (!column_default)
        return false;

    /// collect identifiers required for evaluation
    IdentifierNameSet identifiers;
    column_default->expression->collectIdentifierNames(identifiers);

    bool result = false;
    for (const auto & identifier : identifiers)
        result |= injectRequiredColumnsRecursively(identifier, storage_columns, alter_conversions, part, columns, required_columns, injected_columns);

    return result;
}

}

NameSet injectRequiredColumns(const MergeTreeMetaBase & storage,
    const StorageMetadataPtr & metadata_snapshot, const MergeTreeMetaBase::DataPartPtr & part,
    Names & columns, const String& default_injected_column)
{
    NameSet required_columns{std::begin(columns), std::end(columns)};
    NameSet injected_columns;

    bool have_at_least_one_physical_column = false;

    const auto & storage_columns = metadata_snapshot->getColumns();
    MergeTreeMetaBase::AlterConversions alter_conversions;
    if (!part->isProjectionPart())
        alter_conversions = storage.getAlterConversionsForPart(part);
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i] == "_part_row_number" || columns[i] == RowExistsColumn::ROW_EXISTS_COLUMN.name)
        {
            /// _part_row_number is read by IMergeTreeReader::readRows, just like other physical columns
            have_at_least_one_physical_column = true;
            continue;
        }

        auto name_in_storage = Nested::extractTableName(columns[i]);
        if (storage_columns.has(name_in_storage) && isObject(storage_columns.get(name_in_storage).type))
        {
            have_at_least_one_physical_column = true;
            continue;
        }
        /// We are going to fetch only physical columns
        if (!storage_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, columns[i]))
            throw Exception("There is no physical column or subcolumn " + columns[i] + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        have_at_least_one_physical_column |= injectRequiredColumnsRecursively(
            columns[i], storage_columns, alter_conversions,
            part, columns, required_columns, injected_columns);
    }

    /** Add a column of the minimum size.
        * Used in case when no column is needed or files are missing, but at least you need to know number of rows.
        * Adds to the columns.
        */
    if (!have_at_least_one_physical_column)
    {
        /// todo(weiping): temporariliy skip low cardinality for default injected column
        if (!default_injected_column.empty() && !storage_columns.getColumnOrSubcolumn(GetColumnsOptions::AllPhysical, default_injected_column).getTypeInStorage()->lowCardinality())
        {
            columns.push_back(default_injected_column);
        }
        else
        {
            const auto minimum_size_column_name = part->getColumnNameWithMinimumCompressedSize(metadata_snapshot);
            columns.push_back(minimum_size_column_name);
        }
        /// correctly report added column
        injected_columns.insert(columns.back());
    }

    return injected_columns;
}


MergeTreeReadTask::MergeTreeReadTask(
    const MergeTreeMetaBase::DataPartPtr & data_part_, ImmutableDeleteBitmapPtr delete_bitmap_, const MarkRanges & mark_ranges_once_read_, const size_t part_index_in_query_,
    const Names & ordered_names_, const NameSet & column_name_set_,
    const MergeTreeReadTaskColumns & task_columns_,
    bool remove_prewhere_column_, bool should_reorder_,
    MergeTreeBlockSizePredictorPtr && size_predictor_, const MarkRanges & mark_ranges_total_read_)
    : data_part{data_part_}, delete_bitmap{std::move(delete_bitmap_)}, mark_ranges_once_read{mark_ranges_once_read_}, part_index_in_query{part_index_in_query_},
    ordered_names{ordered_names_}, column_name_set{column_name_set_}, task_columns{task_columns_},
    remove_prewhere_column{remove_prewhere_column_}, should_reorder{should_reorder_}, size_predictor{std::move(size_predictor_)}, mark_ranges_total_read(mark_ranges_total_read_)
{
}

MergeTreeReadTask::~MergeTreeReadTask() = default;


static size_t getColumnsSize(const ColumnPtr & column, bool size_predictor_estimate_lc_size_by_fullstate)
{
    size_t byte_size = column->byteSize();

    if (size_predictor_estimate_lc_size_by_fullstate)
    {
        auto * lc_col = typeid_cast<const ColumnLowCardinality *>(column.get());
        if (lc_col && !lc_col->isFullState())
        {
            /// If lc contains a huge dictionary, limit to read a smaller rows may still get a big dictionary,
            /// and makes average bytes of each rows larger and larger. So we assuming the column is in fullstate, so the result
            /// of average bytes of each rows will converge to a stable value (max_bytes / avg_bytes_of_each_dict_key).
            size_t approx_total_bytes_in_fullstate = lc_col->size() * lc_col->getDictionary().byteSize()
                / std::max(1UL, lc_col->getDictionary().size());
            byte_size = std::min(byte_size, approx_total_bytes_in_fullstate);
        }
    }

    return byte_size;
}


MergeTreeBlockSizePredictor::MergeTreeBlockSizePredictor(
    const MergeTreeMetaBase::DataPartPtr & data_part_, const Names & columns, const Block & sample_block)
    : data_part(data_part_)
{
    number_of_rows_in_part = data_part->rows_count;
    /// Initialize with sample block until update won't called.
    initialize(sample_block, {}, columns);
}

void MergeTreeBlockSizePredictor::initialize(
    const Block & sample_block,
    const Columns & columns,
    const Names & names,
    bool from_update,
    bool size_predictor_estimate_lc_size_by_fullstate)
{
    fixed_columns_bytes_per_row = 0;
    dynamic_columns_infos.clear();

    std::unordered_set<String> names_set;
    if (!from_update)
        names_set.insert(names.begin(), names.end());

    size_t num_columns = sample_block.columns();
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column_with_type_and_name = sample_block.getByPosition(pos);
        const String & column_name = column_with_type_and_name.name;
        const ColumnPtr & column_data = from_update ? columns[pos]
                                                    : column_with_type_and_name.column;

        if (!from_update && !names_set.count(column_name))
            continue;

        /// At least PREWHERE filter column might be const.
        if (typeid_cast<const ColumnConst *>(column_data.get()))
            continue;

        if (column_data->valuesHaveFixedSize())
        {
            size_t size_of_value = column_data->sizeOfValueIfFixed();
            fixed_columns_bytes_per_row += column_data->sizeOfValueIfFixed();
            max_size_per_row_fixed = std::max<size_t>(max_size_per_row_fixed, size_of_value);
        }
        else
        {
            ColumnInfo info;
            info.name = column_name;
            /// If column isn't fixed and doesn't have checksum, than take first
            ColumnSize column_size = data_part->getColumnSize(
                column_name, *column_with_type_and_name.type);

            info.bytes_per_row_global = column_size.data_uncompressed
                ? column_size.data_uncompressed / number_of_rows_in_part
                : getColumnsSize(column_data, size_predictor_estimate_lc_size_by_fullstate) / std::max<size_t>(1, column_data->size());

            dynamic_columns_infos.emplace_back(info);
        }
    }

    bytes_per_row_global = fixed_columns_bytes_per_row;
    for (auto & info : dynamic_columns_infos)
    {
        info.bytes_per_row = info.bytes_per_row_global;
        bytes_per_row_global += info.bytes_per_row_global;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
    bytes_per_row_current = bytes_per_row_global;
}

void MergeTreeBlockSizePredictor::startBlock()
{
    block_size_bytes = 0;
    block_size_rows = 0;
    for (auto & info : dynamic_columns_infos)
        info.size_bytes = 0;
}


/// TODO: add last_read_row_in_part parameter to take into account gaps between adjacent ranges
void MergeTreeBlockSizePredictor::update(
    const Block & sample_block,
    const Columns & columns,
    size_t num_rows,
    bool size_predictor_estimate_lc_size_by_fullstate,
    double decay)
{
    if (columns.size() != sample_block.columns())
        throw Exception("Inconsistent number of columns passed to MergeTreeBlockSizePredictor. "
                        "Have " + toString(sample_block.columns()) + " in sample block "
                        "and " + toString(columns.size()) + " columns in list", ErrorCodes::LOGICAL_ERROR);

    if (!is_initialized_in_update)
    {
        /// Reinitialize with read block to update estimation for DEFAULT and MATERIALIZED columns without data.
        initialize(sample_block, columns, {}, true, size_predictor_estimate_lc_size_by_fullstate);
        is_initialized_in_update = true;
    }

    if (num_rows < block_size_rows)
    {
        throw Exception("Updated block has less rows (" + toString(num_rows) + ") than previous one (" + toString(block_size_rows) + ")",
                        ErrorCodes::LOGICAL_ERROR);
    }

    size_t diff_rows = num_rows - block_size_rows;
    block_size_bytes = num_rows * fixed_columns_bytes_per_row;
    bytes_per_row_current = fixed_columns_bytes_per_row;
    block_size_rows = num_rows;

    /// Make recursive updates for each read row: v_{i+1} = (1 - decay) v_{i} + decay v_{target}
    /// Use sum of geometric sequence formula to update multiple rows: v{n} = (1 - decay)^n v_{0} + (1 - (1 - decay)^n) v_{target}
    /// NOTE: DEFAULT and MATERIALIZED columns without data has inaccurate estimation of v_{target}
    double alpha = std::pow(1. - decay, diff_rows);

    max_size_per_row_dynamic = 0;
    for (auto & info : dynamic_columns_infos)
    {
        size_t new_size = getColumnsSize(columns[sample_block.getPositionByName(info.name)], size_predictor_estimate_lc_size_by_fullstate);
        size_t diff_size = new_size - std::min(new_size, info.size_bytes);

        double local_bytes_per_row = static_cast<double>(diff_size) / diff_rows;
        info.bytes_per_row = alpha * info.bytes_per_row + (1. - alpha) * local_bytes_per_row;

        info.size_bytes = new_size;
        block_size_bytes += new_size;
        bytes_per_row_current += info.bytes_per_row;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
}


MergeTreeReadTaskColumns getReadTaskColumns(
    const MergeTreeMetaBase & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeMetaBase::DataPartPtr & data_part,
    const Names & required_columns,
    const PrewhereInfoPtr & prewhere_info,
    const MergeTreeIndexContextPtr & index_context,
    const std::deque<AtomicPredicatePtr> & atomic_predicates,
    bool check_columns)
{
    Names column_names = required_columns;
    Names pre_column_names;
    std::vector<Names> per_stage_column_names;
    std::vector<NameSet> per_stage_bitmap_nameset;

    /// inject columns required for defaults evaluation
    bool should_reorder = !injectRequiredColumns(storage, storage_snapshot->getMetadataForQuery(), data_part, column_names, "").empty();
    MergeTreeReadTaskColumns result;

    if (!atomic_predicates.empty())
    {
        NameSet tbl_columns(column_names.begin(), column_names.end());
        NameSet pre_name_set;
        NameSet bitmap_name_set;
        /// Start from lowest stage in chain
        for (size_t i = atomic_predicates.size() - 1; i > 0; --i)
        {
            const auto & p = atomic_predicates[i];
            if (!p) continue;
            auto cols = p->predicate_actions->getRequiredColumnsNames();
            auto * bitmap_index_info = p->index_context ? dynamic_cast<BitmapIndexInfo *>(p->index_context->get(MergeTreeIndexInfo::Type::BITMAP).get()) : nullptr;
            auto bitmap_index_columns = bitmap_index_info ? bitmap_index_info->getIndexColumns(data_part) : std::pair<NameSet,NameSet>{};
            const auto & index_names = bitmap_index_columns.first;
            auto & bitmap_names = bitmap_index_columns.second;
            bitmap_name_set.insert(bitmap_names.begin(), bitmap_names.end());
            /// only consider columns from table
            std::erase_if(cols, [&tbl_columns](const String & name) {return !tbl_columns.contains(name); });
            /// In case we have both row-level policy predicate and normal predicate on same columns,
            /// we cannot merge those predicates together but may need to execute those predicate on
            /// 2 different stages. This logic make sure that the column is read only once. Note that
            /// it's absolutely ok for a stage in chain to not read anything but only execute predicates.
            std::erase_if(cols, [&pre_name_set](const String & name) { return !pre_name_set.emplace(name).second; });
            /// Remove bitmap-indexed columns
            std::erase_if(cols,[&index_names, &p, &bitmap_index_info](auto & col) {
                // it's index column, and not in non removable
                return index_names.count(col) > 0 && (!p || !bitmap_index_info || !bitmap_index_info->non_removable_index_columns.contains(col));
            });
            /// for default column read
            if (!cols.empty())
            {
                const auto injected_pre_columns = injectRequiredColumns(storage, storage_snapshot->getMetadataForQuery(), data_part, cols, "");
                tbl_columns.insert(injected_pre_columns.begin(), injected_pre_columns.end());
                if (!injected_pre_columns.empty())
                    should_reorder = true;
            }
            per_stage_column_names.emplace_back(std::move(cols));
            per_stage_bitmap_nameset.emplace_back(std::move(bitmap_names));
        }

        /// No need for injected columns here, just let reader create default values for missing columns
        Names final_stage_column_names;
        for (const auto & name : column_names)
            if (!pre_name_set.count(name))
            final_stage_column_names.push_back(name);

        /// Handle the last column group
        const auto & p = atomic_predicates[0];
        auto * bitmap_index_info = p->index_context ? dynamic_cast<BitmapIndexInfo *>(p->index_context->get(MergeTreeIndexInfo::Type::BITMAP).get()) : nullptr;
        auto bitmap_index_columns = bitmap_index_info ? bitmap_index_info->getIndexColumns(data_part) : std::pair<NameSet,NameSet>{};
        const auto & index_names = bitmap_index_columns.first;
        auto & bitmap_names = bitmap_index_columns.second;
        /// last stage for default column read
        if (!final_stage_column_names.empty())
        {
            const auto injected_pre_columns = injectRequiredColumns(storage, storage_snapshot->getMetadataForQuery(), data_part, final_stage_column_names, "");
            tbl_columns.insert(injected_pre_columns.begin(), injected_pre_columns.end());
            if (!injected_pre_columns.empty())
                should_reorder = true;
        }
        /// For last stage, bitmap info can contain bitmap columns from previous stage
        for (const auto & name : bitmap_name_set)
        {
            if (auto it = bitmap_names.find(name); it != bitmap_names.end())
            bitmap_names.erase(it);
        }
        std::erase_if(final_stage_column_names,[&index_names, &p, &bitmap_index_info](auto & col) {
            // it's index column, and not in non removable
            return index_names.count(col) > 0 && (!p || !bitmap_index_info || !bitmap_index_info->non_removable_index_columns.contains(col));
        });
        per_stage_column_names.emplace_back(std::move(final_stage_column_names));
        per_stage_bitmap_nameset.emplace_back(std::move(bitmap_names));

        std::reverse(per_stage_column_names.begin(), per_stage_column_names.end());
        std::reverse(per_stage_bitmap_nameset.begin(), per_stage_bitmap_nameset.end());
        result.per_stage_bitmap_nameset = std::move(per_stage_bitmap_nameset);
    }
    else
    {
        if (prewhere_info)
        {
            if (prewhere_info->alias_actions)
            pre_column_names = prewhere_info->alias_actions->getRequiredColumnsNames();
            else
            {
            pre_column_names = prewhere_info->prewhere_actions->getRequiredColumnsNames();

            if (prewhere_info->row_level_filter)
            {
                NameSet names(pre_column_names.begin(), pre_column_names.end());

                for (auto & name : prewhere_info->row_level_filter->getRequiredColumnsNames())
                {
                    if (names.count(name) == 0)
                        pre_column_names.push_back(name);
                }
            }
            }

            if (pre_column_names.empty())
            pre_column_names.push_back(column_names[0]);

            const auto injected_pre_columns = injectRequiredColumns(storage, storage_snapshot->getMetadataForQuery(), data_part, pre_column_names, "");
            if (!injected_pre_columns.empty())
                should_reorder = true;

            const NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());

            Names post_column_names;
            for (const auto & name : column_names)
            if (!pre_name_set.count(name))
                post_column_names.push_back(name);

            column_names = post_column_names;
        }

        NameSet bitmap_column_names;
        NameSet bitmap_pre_column_names;
        NameSet index_name_set;

        auto * bitmap_index_info
            = index_context ? dynamic_cast<BitmapIndexInfo *>(index_context->get(MergeTreeIndexInfo::Type::BITMAP).get()) : nullptr;
        std::tie(index_name_set, bitmap_column_names)
            = bitmap_index_info ? bitmap_index_info->getIndexColumns(data_part) : std::pair<NameSet, NameSet>{};
        if (!bitmap_column_names.empty())
        {
            // remove the indexed columns from prewhere_columns and tasks columns to prevent from being read.
            std::erase_if(pre_column_names, [&index_name_set, &bitmap_index_info](auto & col) {
                // it's index column, and not in non removable
                return index_name_set.count(col) > 0 && bitmap_index_info->non_removable_index_columns.count(col) <= 0;
            });
            std::erase_if(column_names, [&index_name_set, &bitmap_index_info](auto & col) {
                return index_name_set.count(col) > 0 && bitmap_index_info->non_removable_index_columns.count(col) <= 0;
            });

            /// Collect the bitmap index columns for prewhere, no need to check for ready because it's subset of bitmap_index_info
            /// and should be ready at this point
            if (prewhere_info && prewhere_info->index_context)
            {
                auto * prewhere_bitmap_index_info = dynamic_cast<BitmapIndexInfo *>(prewhere_info->index_context->get(MergeTreeIndexInfo::Type::BITMAP).get());
                {
                    const auto & index_names = prewhere_bitmap_index_info->index_names;
                    for (const auto & index_name : index_names)
                    {
                        bitmap_pre_column_names.emplace(index_name.first);
                    }
                }
                /// Do not read same bitmap columns in both prewhere and where
                std::erase_if(bitmap_column_names, [&bitmap_pre_column_names](const auto & name){
                    return bitmap_pre_column_names.contains(name);
                });
            }
        }

        result.bitmap_index_pre_columns = std::move(bitmap_pre_column_names);
        result.bitmap_index_columns = std::move(bitmap_column_names);
    }
    if (check_columns)
    {
        auto options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withExtendedObjects();
        result.pre_columns = storage_snapshot->getColumnsByNames(options, pre_column_names);
        for (auto & names : per_stage_column_names)
            result.per_stage_columns.emplace_back(storage_snapshot->getColumnsByNames(options, names));
        result.columns = storage_snapshot->getColumnsByNames(options, column_names);
    }
    else
    {
        auto columns = data_part->getColumns();
        columns.push_back(NameAndTypePair("_part_row_number", std::make_shared<DataTypeUInt64>()));
        result.pre_columns = columns.addTypes(pre_column_names);
        for (auto & names : per_stage_column_names)
            result.per_stage_columns.emplace_back(columns.addTypes(names));
        result.columns = columns.addTypes(column_names);
    }

    result.should_reorder = should_reorder;

    return result;
}

}
