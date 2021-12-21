#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

namespace JSONBuilder { class JSONMap; }

class ReadBuffer;
class WriteBuffer;

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;
    Names argument_names;    /// used if no `arguments` are specified.
    String column_name;      /// What name to use for a column with aggregate function values

    void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
    void explain(JSONBuilder::JSONMap & map) const;

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};

using AggregateDescriptions = std::vector<AggregateDescription>;

}
