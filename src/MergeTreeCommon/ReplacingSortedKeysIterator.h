#pragma once

#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/IndexFile/IndexFileMergeIterator.h>
#include <Storages/IndexFile/Status.h>

namespace DB
{
/// TODO(GDY) doc
class ReplacingSortedKeysIterator
{
public:
    enum class VersionMode
    {
        ExplicitVersion,
        PartitionValueAsVersion,
        NoVersion,
    };

    struct DeleteInfo
    {
        /// If false, it means this row is insert operation with delete info. Otherwise, it means this row is just delete row.
        bool just_delete_row;
        UInt64 delete_version;
        DeleteInfo(bool just_delete_row_, UInt64 delete_version_) : just_delete_row(just_delete_row_), delete_version(delete_version_) { }
    };
    using DeleteInfoPtr = std::shared_ptr<DeleteInfo>;

    struct RowPos
    {
        RowPos() : child(0), rowid(0), version(0), delete_info(nullptr) { }

        UInt32 child; /// index of child iterator
        UInt32 rowid; /// row number
        UInt64 version;
        DeleteInfoPtr delete_info;
    };
    using DeleteCallback = std::function<void(const RowPos &)>;

    ReplacingSortedKeysIterator(
        const IndexFile::Comparator * comparator_,
        const IMergeTreeDataPartsVector & parts_,
        std::vector<std::unique_ptr<IndexFile::Iterator>> child_iters_,
        DeleteCallback delete_cb_,
        VersionMode version_mode_,
        const ImmutableDeleteBitmapVector & delete_flag_bitmaps_ = {});

    bool Valid() const { return valid; }

    IndexFile::Status status() const { return iter.status(); }

    UInt64 getVisitedRowNum() const { return visited_row_num.load(std::memory_order_relaxed); }

    void SeekToFirst()
    {
        iter.SeekToFirst();
        MoveToNextKey();
    }

    /// REQUIRES: Valid()
    void Next()
    {
        assert(Valid());
        MoveToNextKey();
    }

    /// REQUIRES: Valid()
    const String & CurrentKey() const
    {
        assert(Valid());
        return cur_key;
    }

    const RowPos & CurrentRowPos() const
    {
        assert(Valid());
        return cur_row;
    }

    bool IsCurrentLowPriority() const;

    static bool DecodeRowid(Slice & input, UInt32 & rowid);

    static RowPos decodeCurrentRowPos(
        const IndexFile::IndexFileMergeIterator & iter,
        VersionMode version_mode,
        const IMergeTreeDataPartsVector & child_parts, // only for exception msg
        const std::vector<UInt64> & child_implicit_versions,
        const ImmutableDeleteBitmapVector & delete_flag_bitmaps = {});

private:

    /// Find final record pos of all rows with the same key
    size_t FindFinalPos(std::vector<RowPos> & rows_with_key);

    void MoveToNextKey();

    const IndexFile::Comparator * comparator;
    const IMergeTreeDataPartsVector & parts;
    IndexFile::IndexFileMergeIterator iter;
    DeleteCallback delete_cb;
    VersionMode version_mode;
    std::vector<UInt64> part_implicit_versions;
    const ImmutableDeleteBitmapVector & delete_flag_bitmaps;

    bool valid = false;
    String cur_key; /// cache the data of current key
    RowPos cur_row; /// cache the maximum version of the decoded current row
    std::atomic<UInt64> visited_row_num = 0;  /// cache the visited row num, use to indicate dedup task progress
};

}
