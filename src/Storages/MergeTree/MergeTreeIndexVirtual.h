

#include "Storages/MergeTree/MergeTreeIndices.h"
#include "Storages/MergeTree/RangesInDataPart.h"

namespace DB
{

class MergeTreeIndexVirtual final : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexVirtual(MergeTreeIndexPtr nested_)
        : IMergeTreeIndex(nested_->index), nested(std::move(nested_))
    {
    }
    ~MergeTreeIndexVirtual() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override
    {
        return nested->createIndexGranule();
    }
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override
    {
        return nested->createIndexAggregator();
    }

    MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, ContextPtr context) const override
    {
        return nested->createIndexCondition(query, context);
    }

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override
    {
        return nested->mayBenefitFromIndexForIn(node);
    }

    void materialize(const RangesInDataParts & part_with_ranges)
    {
        
    }

private:
    MergeTreeIndexPtr nested;
};

}
