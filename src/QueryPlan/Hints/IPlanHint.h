#pragma once

#include <memory>
#include <vector>
#include <Parsers/IAST.h>

namespace DB
{

class PlanNodeBase;
class IPlanHint;
using PlanHintPtr = std::shared_ptr<IPlanHint>;

struct HintOptions
{
    Strings table_name_list = {};
    Strings func_names = {};
};

enum class HintCategory
{
    UNKNOWN = 0,
    DISTRIBUTION_TYPE,
    JOIN_ORDER,
    JOIN_ALGORITHM,
    PUSH_PARTIAL_AGG,
};

class IPlanHint
{
public:
    virtual ~IPlanHint() = default;

    virtual String getName() const = 0;
    virtual HintCategory getType() const = 0;
    virtual Strings getOptions() const = 0;

    // Whether a plan hint can attach to a plan node(e.g. a join strategy hint cannot attach to any kind of nodes except JoinNode).
    // This method will be invoked in hint propagation, which happens just after an AST is converted to a QueryPlan.
    virtual bool canAttach(PlanNodeBase & node, HintOptions & hint_options) const = 0;

};

}

