#include <CloudServices/CnchPartsHelper.h>

#include <Catalog/DataModelPartWrapper.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <sstream>

namespace DB::CnchPartsHelper
{
LoggingOption getLoggingOption(const Context & c)
{
    return (c.getSettingsRef().send_logs_level == LogsLevel::none) ? DisableLogging : EnableLogging;
}

namespace
{
    template <class T>
    std::string partsToDebugString(const std::vector<T> & parts)
    {
        std::ostringstream oss;
        for (auto & p : parts)
            oss << p->get_name() << " d=" << p->get_deleted() << " p=" << bool(p->get_info().hint_mutation)
                << " h=" << p->get_info().hint_mutation << '\n';
        return oss.str();
    }

    template <class T>
    struct PartComparator
    {
        bool operator()(const T & lhs, const T & rhs) const
        {
            auto & l = lhs->get_info();
            auto & r = rhs->get_info();
            return std::forward_as_tuple(l.partition_id, l.min_block, l.max_block, l.level, lhs->get_commit_time(), l.storage_type)
                < std::forward_as_tuple(r.partition_id, r.min_block, r.max_block, r.level, rhs->get_commit_time(), r.storage_type);
        }
    };

    /** Cnch parts classification:
     *  all parts:
     *  1) invisible parts
     *      a) covered by new version parts
     *      b) covered by DROP_RANGE  (invisible_dropped_parts)
     *  2) visible parts
     *      a) with data
     *      b) without data, i.e. DROP_RANGE
     *          i) alone  (visible_alone_drop_ranges)
     *          ii) not alone
     */

    template <class Vec>
    Vec calcVisiblePartsImpl(
        Vec & all_parts,
        bool flatten,
        bool skip_drop_ranges,
        Vec * visible_alone_drop_ranges,
        Vec * invisible_dropped_parts,
        LoggingOption logging)
    {
        using Part = typename Vec::value_type;

        Vec visible_parts;

        if (all_parts.empty())
            return visible_parts;

        if (all_parts.size() == 1)
        {
            if (skip_drop_ranges && all_parts.front()->get_deleted())
                ; /// do nothing
            else
                visible_parts = all_parts;

            if (visible_alone_drop_ranges && all_parts.front()->get_deleted())
                *visible_alone_drop_ranges = all_parts;
            return visible_parts;
        }

        std::sort(all_parts.begin(), all_parts.end(), PartComparator<Part>{});

        /// One-pass algorithm to construct delta chains
        auto prev_it = all_parts.begin();
        auto curr_it = std::next(prev_it);

        while (prev_it != all_parts.end())
        {
            auto & prev_part = *prev_it;

            /// 1. prev_part is a DROP RANGE mark
            if (prev_part->get_info().level == MergeTreePartInfo::MAX_LEVEL)
            {
                /// a. curr_part is in same partition
                if (curr_it != all_parts.end() && prev_part->get_info().partition_id == (*curr_it)->get_info().partition_id)
                {
                    /// i) curr_part is also a DROP RANGE mark, and must be the bigger one
                    if ((*curr_it)->get_info().level == MergeTreePartInfo::MAX_LEVEL)
                    {
                        if (invisible_dropped_parts)
                            invisible_dropped_parts->push_back(*prev_it);

                        if (visible_alone_drop_ranges)
                        {
                            (*prev_it)->setPreviousPart(nullptr); /// reset whatever
                            (*curr_it)->setPreviousPart(*prev_it); /// set previous part for visible_alone_drop_ranges
                        }

                        prev_it = curr_it;
                        ++curr_it;
                        continue;
                    }
                    /// ii) curr_part is marked as dropped by prev_part
                    else if ((*curr_it)->get_info().max_block <= prev_part->get_info().max_block)
                    {
                        if (invisible_dropped_parts)
                            invisible_dropped_parts->push_back(*curr_it);

                        if (visible_alone_drop_ranges)
                            prev_part->setPreviousPart(*curr_it); /// set previous part for visible_alone_drop_ranges

                        ++curr_it;
                        continue;
                    }
                }

                /// a. iii) [fallthrough] same partition, but curr_part is a new part with data after the DROP RANGE mark

                /// b) curr_it is in the end
                /// c) different partition

                if (skip_drop_ranges)
                    ; /// do nothing
                else
                    visible_parts.push_back(prev_part);

                if (visible_alone_drop_ranges && !prev_part->tryGetPreviousPart())
                    visible_alone_drop_ranges->push_back(prev_part);
                prev_part->setPreviousPart(nullptr);
            }
            /// 2. curr_part contains the prev_part
            else if (curr_it != all_parts.end() && (*curr_it)->containsExactly(*prev_part))
            {
                (*curr_it)->setPreviousPart(prev_part);
            }
            /// 3. curr_it is in the end
            /// 4. curr_part is not related to the prev_part which means prev_part must be visible
            else
            {
                if (skip_drop_ranges && prev_part->get_deleted())
                    ; /// do nothing
                else
                    visible_parts.push_back(prev_part);

                if (visible_alone_drop_ranges && !prev_part->tryGetPreviousPart() && prev_part->get_deleted())
                    visible_alone_drop_ranges->push_back(prev_part);
            }

            prev_it = curr_it;
            if (curr_it != all_parts.end())
                ++curr_it;
        }

        if (flatten)
            flattenPartsVector(visible_parts);

        if (logging == EnableLogging)
        {
            auto log = &Poco::Logger::get(__func__);
            LOG_DEBUG(log, "all_parts:\n {}", partsToDebugString(all_parts));
            LOG_DEBUG(log, "visible_parts (skip_drop_ranges={}):\n{}", skip_drop_ranges, partsToDebugString(visible_parts));
            if (visible_alone_drop_ranges)
                LOG_DEBUG(log, "visible_alone_drop_ranges:\n{}", partsToDebugString(*visible_alone_drop_ranges));
            if (invisible_dropped_parts)
                LOG_DEBUG(log, "invisible_dropped_parts:\n{}", partsToDebugString(*invisible_dropped_parts));
        }

        return visible_parts;
    }

} /// end of namespace

MergeTreeDataPartsVector calcVisibleParts(MergeTreeDataPartsVector & all_parts, bool flatten, LoggingOption logging)
{
    return calcVisiblePartsImpl<MergeTreeDataPartsVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging);
}

ServerDataPartsVector calcVisibleParts(ServerDataPartsVector & all_parts, bool flatten, LoggingOption logging)
{
    return calcVisiblePartsImpl<ServerDataPartsVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging);
}

ServerDataPartsVector calcVisiblePartsForGC(
    ServerDataPartsVector & all_parts,
    ServerDataPartsVector * visible_alone_drop_ranges,
    ServerDataPartsVector * invisible_dropped_parts,
    LoggingOption logging)
{
    return calcVisiblePartsImpl(
        all_parts,
        /* flattern */ false,
        /* skip_drop_ranges */ false,
        visible_alone_drop_ranges,
        invisible_dropped_parts,
        logging);
}
}
