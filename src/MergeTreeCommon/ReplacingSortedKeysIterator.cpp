#include <Common/Coding.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <MergeTreeCommon/ReplacingSortedKeysIterator.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

bool DecodeVersion(Slice & input, UInt64 & version)
{
    if (input.size() >= sizeof(UInt64))
    {
        version = DecodeFixed64(input.data());
        input.remove_prefix(sizeof(UInt64));
        return true;
    }
    return false;
}

bool ReplacingSortedKeysIterator::DecodeRowid(Slice & input, UInt32 & rowid)
{
    return GetVarint32(&input, &rowid);
}

ReplacingSortedKeysIterator::RowPos ReplacingSortedKeysIterator::decodeCurrentRowPos(
    const IndexFile::IndexFileMergeIterator & iter,
    VersionMode version_mode,
    const IMergeTreeDataPartsVector & child_parts, // only for exception msg
    const std::vector<UInt64> & child_implicit_versions,
    const ImmutableDeleteBitmapVector & delete_flag_bitmaps)
{
    RowPos res;
    res.child = iter.child_index();
    Slice value = iter.value();
    if (!DecodeRowid(value, res.rowid))
        throw Exception("Can't decode row number from part " + child_parts[res.child]->name, ErrorCodes::CORRUPTED_DATA);
    res.version = child_implicit_versions[res.child];
    if (version_mode == VersionMode::ExplicitVersion)
    {
        if (!DecodeVersion(value, res.version))
            throw Exception("Can't decode version from part " + child_parts[res.child]->name, ErrorCodes::CORRUPTED_DATA);
    }

    /// Check whether it's a delete row
    if (res.child < delete_flag_bitmaps.size() && delete_flag_bitmaps[res.child] && delete_flag_bitmaps[res.child]->contains(res.rowid))
        res.delete_info = std::make_shared<DeleteInfo>(/*just_delete_row*/ true, res.version);
    return res;
}

ReplacingSortedKeysIterator::ReplacingSortedKeysIterator(
    const IndexFile::Comparator * comparator_,
    const IMergeTreeDataPartsVector & parts_,
    std::vector<std::unique_ptr<IndexFile::Iterator>> child_iters_,
    DeleteCallback delete_cb_,
    VersionMode version_mode_,
    const ImmutableDeleteBitmapVector & delete_flag_bitmaps_)
    : comparator(comparator_)
    , parts(parts_)
    , iter(comparator, std::move(child_iters_))
    , delete_cb(delete_cb_)
    , version_mode(version_mode_)
    , part_implicit_versions(parts.size(), 0)
    , delete_flag_bitmaps(delete_flag_bitmaps_)
{
    if (version_mode == VersionMode::PartitionValueAsVersion)
    {
        for (size_t i = 0; i < parts.size(); ++i)
            part_implicit_versions[i] = parts[i]->getVersionFromPartition();
    }
}

bool ReplacingSortedKeysIterator::IsCurrentLowPriority() const
{
    return parts[cur_row.child]->low_priority;
}

size_t ReplacingSortedKeysIterator::FindFinalPos(std::vector<RowPos> & rows_with_key)
{
    if (rows_with_key.size() == 1)
        return 0;
    size_t max_pos = 0;
    if (version_mode == VersionMode::NoVersion)
        max_pos = rows_with_key.size() - 1;
    else
    {
    /*************************************************************************************************************************
        * When there has version and delete_flag, need to handle the following cases between multiple invisible parts:
        * 1. force delete: when version is set to zero, it means that force delete ignoring version
        *    e.g.(in order)  unique key      version      value       _delete_flag_
        *    visible row:       key1            5            a              0
        *    invisible row1:    key1            0            b              1
        *    invisible row2:    key1            3            c              0
        *    In this case, the correct result is to keep invisible row2. Thus need to return invisible row2 with delete info to force delete visible row.
        * 2. first delete, and then insert a row with smaller version than before
        *    e.g.(in order)  unique key      version      value       _delete_flag_
        *    visible row:       key1            3(5)         a              0
        *    invisible row1:    key1            4            b              1
        *    invisible row2:    key1            3            c              0
        *    In this case, the correct result depends on version of visible row:
        *    a. if version of visible row is 3, need to keep invisible row2.
        *    b. if version of visible row is 5, need to keep visible row.
        *    Thus need to return invisible row2 with delete info.
        * In the above two cases, both need to return row info with delete info.
        *
        * TODO: handle the above two cases in same block in writing process
        ************************************************************************************************************************/
        size_t insert_pos = rows_with_key.size(), delete_pos = rows_with_key.size();
        for (size_t i = 0; i < rows_with_key.size(); ++i)
        {
            const auto & row_info = rows_with_key[i];
            if (row_info.delete_info)
            {
                if (!row_info.delete_info->delete_version) /// force delete
                {
                    delete_pos = i;
                    insert_pos = rows_with_key.size();
                }
                else
                {
                    if (insert_pos < rows_with_key.size()
                        && row_info.delete_info->delete_version < rows_with_key[insert_pos].version)
                        continue;
                    insert_pos = rows_with_key.size();
                    if (delete_pos < rows_with_key.size()
                        && (!rows_with_key[delete_pos].delete_info->delete_version
                            || rows_with_key[delete_pos].delete_info->delete_version >= row_info.delete_info->delete_version))
                        continue;
                    delete_pos = i;
                }
            }
            else
            {
                if (insert_pos < rows_with_key.size() && row_info.version < rows_with_key[insert_pos].version)
                    continue;
                insert_pos = i;
                if (delete_pos < rows_with_key.size()
                    && (!rows_with_key[delete_pos].delete_info->delete_version
                        || rows_with_key[delete_pos].delete_info->delete_version > row_info.version))
                    continue;
                delete_pos = rows_with_key.size();
            }
        }
        assert(insert_pos < rows_with_key.size() || delete_pos < rows_with_key.size());
        if (insert_pos < rows_with_key.size())
        {
            if (delete_pos < rows_with_key.size())
                rows_with_key[insert_pos].delete_info = std::make_shared<DeleteInfo>(
                    /*just_delete_row*/ false, rows_with_key[delete_pos].delete_info->delete_version);
            max_pos = insert_pos;
        }
        else
            max_pos = delete_pos;
    }
    return max_pos;
}

void ReplacingSortedKeysIterator::MoveToNextKey()
{
    valid = iter.Valid();
    if (valid)
    {
        auto slice = iter.key();
        cur_key.assign(slice.data(), slice.size());

        /// positions of all rows having `cur_key` with high priority
        std::vector<RowPos> rows_with_key_high_p;
        /// positions of all rows having `cur_key` with low priority
        std::vector<RowPos> rows_with_key_low_p;

        auto fill_rows = [&](RowPos && row) {
            if (parts[row.child]->low_priority)
                rows_with_key_low_p.push_back(std::move(row));
            else
                rows_with_key_high_p.push_back(std::move(row));
        };
        fill_rows(decodeCurrentRowPos(iter, version_mode, parts, part_implicit_versions, delete_flag_bitmaps));

        /// record pos of all rows with the same key
        do
        {
            iter.Next();
            visited_row_num++;

            if (!iter.Valid() || comparator->Compare(cur_key, iter.key()) != 0)
                break;

            fill_rows(decodeCurrentRowPos(iter, version_mode, parts, part_implicit_versions, delete_flag_bitmaps));
        } while (1);

        auto delete_rows = [&](std::vector<RowPos> & rows_with_key, size_t target_pos) {
            /// mark deleted rows with lower version
            for (size_t i = 0; i < rows_with_key.size(); ++i)
            {
                if (i != target_pos && !rows_with_key[i].delete_info)
                    delete_cb(rows_with_key[i]);
            }
        };

        if (rows_with_key_high_p.empty())
        {
            size_t max_pos = FindFinalPos(rows_with_key_low_p);
            /// Set the row with maximum version as the current row
            cur_row = rows_with_key_low_p[max_pos];
            delete_rows(rows_with_key_low_p, max_pos);
        }
        else
        {
            size_t max_pos = FindFinalPos(rows_with_key_high_p);
            /// Set the row with maximum version as the current row
            cur_row = rows_with_key_high_p[max_pos];
            delete_rows(rows_with_key_high_p, max_pos);
            delete_rows(rows_with_key_low_p, rows_with_key_low_p.size()); /// Remove all rows with low priority
        }
    }
}
}
