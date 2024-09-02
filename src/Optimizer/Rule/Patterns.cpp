#include <Optimizer/Rule/Patterns.h>
#include <QueryPlan/SortingStep.h>

namespace DB::Patterns
{

PatternBuilder topN()
{
    auto result = typeOf(IQueryPlanStep::Type::Sorting);
    result.matchingStep<SortingStep>([&](const SortingStep & s) { return !s.hasPreparedParam() && s.getLimitValue() != 0; });
    return result;
}
PatternBuilder & PatternBuilder::capturedAs(const Capture & capture)
{
    return capturedAs(
        capture, [](const PlanNodePtr & node) { return std::any{node}; }, "`this`");
}

PatternBuilder & PatternBuilder::capturedAs(const Capture & capture, const PatternProperty & property)
{
    return capturedAs(capture, property, "unknown");
}

PatternBuilder & PatternBuilder::capturedAs(const Capture & capture, const PatternProperty & property, const std::string & name)
{
    current = std::make_unique<CapturePattern>(name, property, capture, std::move(current));
    return *this;
}

PatternBuilder & PatternBuilder::matching(PatternPredicate predicate)
{
    return matching(std::move(predicate), "unknown");
}

PatternBuilder & PatternBuilder::matching(PatternPredicate predicate, const std::string &)
{
    if (auto * type_pattern = dynamic_cast<TypeOfPattern *>(current.get()))
    {
        if (type_pattern->attaching_predicate)
           throw Exception(
                "TypeOfPattern already has an attaching_predicate", ErrorCodes::LOGICAL_ERROR);
        else
            type_pattern->attaching_predicate = predicate;
    }
    else if (auto * capture_pattern = dynamic_cast<CapturePattern *>(current.get()))
    {
        if (capture_pattern->attaching_predicate)
           throw Exception(
                "CapturePattern already has an attaching_predicate", ErrorCodes::LOGICAL_ERROR);
        else
            capture_pattern->attaching_predicate = predicate;
    }
    else
    {
        throw Exception(
            "Previous pattern of FilterPattern must be a TypeOfPattern/CapturePattern", ErrorCodes::LOGICAL_ERROR);
    }

    return *this;
}

PatternBuilder & PatternBuilder::matchingCapture(std::function<bool(const Captures &)> capture_predicate)
{
    return matchingCapture(std::move(capture_predicate), "unknown");
}

PatternBuilder & PatternBuilder::matchingCapture(std::function<bool(const Captures &)> capture_predicate, const std::string & name)
{
    return matching(std::bind(capture_predicate, std::placeholders::_2), name); // NOLINT(modernize-avoid-bind)
}

PatternBuilder & PatternBuilder::withEmpty()
{
    current = std::make_unique<WithPattern>(PatternQuantifier::EMPTY, nullptr, std::move(current));
    return *this;
}

PatternBuilder & PatternBuilder::withSingle(PatternPtr sub_pattern)
{
    current = std::make_unique<WithPattern>(PatternQuantifier::SINGLE, std::move(sub_pattern), std::move(current));
    return *this;
}

PatternBuilder & PatternBuilder::withAny(PatternPtr sub_pattern)
{
    current = std::make_unique<WithPattern>(PatternQuantifier::ANY, std::move(sub_pattern), std::move(current));
    return *this;
}

PatternBuilder & PatternBuilder::withAll(PatternPtr sub_pattern)
{
    current = std::make_unique<WithPattern>(PatternQuantifier::ALL, std::move(sub_pattern), std::move(current));
    return *this;
}

PatternBuilder & PatternBuilder::with(PatternPtrs sub_patterns)
{
    current = std::make_unique<WithPattern>(PatternQuantifier::ALL, std::move(sub_patterns), std::move(current));
    return *this;
}

PatternBuilder & PatternBuilder::oneOf(PatternPtrs sub_patterns)
{
    current = std::make_unique<OneOfPattern>(std::move(sub_patterns), std::move(current));
    return *this;
}
}
