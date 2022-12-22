#pragma once
#include <QueryPlan/PlanVisitor.h>

namespace DB
{
class PlanNodeCardinality
{
public:
    struct Range
    {
        Range(size_t lowerBound_, size_t upperBound_) : lowerBound(lowerBound_), upperBound(upperBound_) { }
        size_t lowerBound;
        size_t upperBound;
    };

    static bool isScalar(PlanNodeBase & node) { return isScalar(extractCardinality(node)); }
    static bool isEmpty(PlanNodeBase & node) { return isEmpty(extractCardinality(node)); }
    static bool isAtMost(PlanNodeBase & node, size_t maxCardinality) { return extractCardinality(node).upperBound < maxCardinality; }
    static bool isAtLeast(PlanNodeBase & node, size_t minCardinality) { return extractCardinality(node).lowerBound > minCardinality; }
    static Range extractCardinality(PlanNodeBase & node);

private:
    static inline bool isScalar(const Range & range) { return range.lowerBound == 1 && range.upperBound == 1; }
    static inline bool isEmpty(const Range & range) { return range.lowerBound == 0 && range.upperBound == 0; }
    static Range intersection(const Range & range, const Range & other)
    {
        return Range{std::max(range.lowerBound, other.lowerBound), std::min(range.lowerBound, other.upperBound)};
    }

    class Visitor;
};

}
