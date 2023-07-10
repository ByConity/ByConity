#include <Common/assert_cast.h>
#include <Interpreters/NullableUtils.h>


namespace DB
{

ColumnPtr extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ConstNullMapPtr & null_map, const std::vector<bool> *null_safeColumns)
{
    ColumnPtr null_map_holder;

    /* one additional null_map column was added for each null safe column, size always >= 2 */
    if (key_columns.size() == 1)
    {
        auto & column = key_columns[0];
        if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(*column))
        {
            null_map_holder = column_nullable->getNullMapColumnPtr();
            null_map = &column_nullable->getNullMapData();
            column = &column_nullable->getNestedColumn();
        }
    }
    else
    {
        /* mapping with null_map columns for null safe columns */
        std::vector<bool> expanded_null_safes;
        if (null_safeColumns) {
            for (bool b : *null_safeColumns) {
                expanded_null_safes.push_back(false);
                if (b)
                    expanded_null_safes.push_back(true);
            }
            null_safeColumns = &expanded_null_safes;
        }

        for (size_t n = 0; n < key_columns.size(); n++)
        {
            auto & column = key_columns[n];

            /* do not construct null_map for null safe column */
            if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(*column))
            {
                column = &column_nullable->getNestedColumn();

                if (!null_map_holder)
                {
                    if (null_safeColumns && (*null_safeColumns)[n])
                        null_map_holder = ColumnUInt8::create(column->size(), 0);
                    else
                        null_map_holder = column_nullable->getNullMapColumnPtr();
                }
                else
                {
                    if (null_safeColumns && (*null_safeColumns)[n])
                        continue;

                    MutableColumnPtr mutable_null_map_holder = IColumn::mutate(std::move(null_map_holder));

                    PaddedPODArray<UInt8> & mutable_null_map = assert_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
                    const PaddedPODArray<UInt8> & other_null_map = column_nullable->getNullMapData();
                    for (size_t i = 0, size = mutable_null_map.size(); i < size; ++i)
                        mutable_null_map[i] |= other_null_map[i];

                    null_map_holder = std::move(mutable_null_map_holder);
                }
            }
        }

        null_map = null_map_holder ? &assert_cast<const ColumnUInt8 &>(*null_map_holder).getData() : nullptr;
    }

    return null_map_holder;
}

}
