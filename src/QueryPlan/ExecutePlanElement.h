#pragma once
#include <memory>
#include <QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/IStorage_fwd.h>
#include <QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/MergeTreeMeta.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/ReadFromMergeTree.h>

namespace DB
{

/// interfaces
// parts within a same group have the same columns & available projections
struct PartGroup
{
    MergeTreeData::DataPartsVector parts;
    bool has_bitmap_index = false;

    bool hasProjection(const String & projection_name) const
    {
        return parts.front()->hasProjection(projection_name);
    }

    bool hasBitmapIndx() const
    {
        return has_bitmap_index;
    }

    const NamesAndTypesList & getColumns() const
    {
        return parts.front()->getColumns();
    }

    String toString() const
    {
        std::stringstream ss;
        ss << '{';
        for (const auto & part: parts)
            ss << part->getNameWithState() << "";
        ss << '}';
        return ss.str();
    }

    size_t partsNum() const
    {
        return parts.size();
    }
};

using PartGroups = std::vector<PartGroup>;

struct ExecutePlanElement
{
    PartGroup part_group;
    MergeTreeDataSelectAnalysisResultPtr read_analysis;

    // if not use projection, below members are not empty
    MergeTreeData::DataPartsVector parts;

    // if use projection, below members are not empty
    ProjectionDescriptionRawPtr projection_desc = nullptr;
    Names projection_required_columns;
    std::shared_ptr<ProjectionStep> rewritten_projection_step;
    std::shared_ptr<FilterStep> rewritten_filter_step;
    ActionsDAGPtr prewhere_actions;
    ActionsDAGPtr projection_index_actions;
    bool read_bitmap_index = false;

    ExecutePlanElement(PartGroup part_group_,
                        MergeTreeDataSelectAnalysisResultPtr read_analysis_,
                        MergeTreeData::DataPartsVector parts_)
        : part_group(std::move(part_group_)),
        read_analysis(std::move(read_analysis_)),
        parts(std::move(parts_))
    {}

    ExecutePlanElement(PartGroup part_group_,
                        MergeTreeDataSelectAnalysisResultPtr read_analysis_,
                        ProjectionDescriptionRawPtr projection_desc_,
                        Names required_columns_,
                        std::shared_ptr<ProjectionStep> projection_,
                        std::shared_ptr<FilterStep> filter_,
                        ActionsDAGPtr prewhere_actions_)
        : part_group(std::move(part_group_)),
        read_analysis(std::move(read_analysis_)),
        projection_desc(projection_desc_),
        projection_required_columns(std::move(required_columns_)),
        rewritten_projection_step(std::move(projection_)),
        rewritten_filter_step(std::move(filter_)),
        prewhere_actions(std::move(prewhere_actions_))
    {}

    String toString() const
    {
        std::ostringstream os;
        if (!read_analysis->error())
            os << "Read Marks: " << read_analysis->marks() << std::endl;

        if (projection_desc)
        {
            os << "Used Projection: " << projection_desc->name << std::endl;
            os << "Projection Type: " << (projection_desc->type == ProjectionDescription::Type::Aggregate ? "aggregate" : "normal" )<< std::endl;

            if (rewritten_filter_step)
            {
                auto filter_dump = serializeAST(*rewritten_filter_step->getFilter());
                os << "Rewritten Filter: " << filter_dump << std::endl;
            }

            if (rewritten_projection_step)
            {
                os << "Column Mapping: " << std::endl;
                auto assignment_dump = rewritten_projection_step->getAssignments().toString();
                os << assignment_dump;
            }
        }

        if (projection_index_actions)
        {
            os << "Projection index: " << projection_index_actions->dumpDAG();
        }

        return os.str();
    }
};

class ExecutePlan: public std::vector<ExecutePlanElement>
{};

}
