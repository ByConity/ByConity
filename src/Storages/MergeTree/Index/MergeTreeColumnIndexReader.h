#pragma once

#include <Core/NamesAndTypes.h>
#include <Columns/IColumn.h>
#include "Storages/MergeTree/MarkRange.h"

namespace DB
{

class Block;
class MergeTreeColumnIndexReader
{
public:
    MergeTreeColumnIndexReader() = default;

    virtual String getName() const { return "MergeTreeColumnIndexReader"; }
    virtual bool validIndexReader() const { return false; }
    virtual size_t read(size_t  /*from_mark*/, bool  /*continue_reading*/, size_t  /*max_rows_to_read*/, Columns &  /*res*/) = 0;

    virtual void initIndexes(const NameSet & columns) = 0;

    virtual void addSegmentIndexesFromMarkRanges(const MarkRanges & mark_ranges_inc) = 0;

    virtual ~MergeTreeColumnIndexReader() = default;

private:

};

using MergeTreeColumnIndexReaderPtr = std::shared_ptr<MergeTreeColumnIndexReader>;

}
