#pragma once
#include <memory>
#include <utility>
#include <Core/Types.h>
#include <Interpreters/Context.h>

namespace DB
{

class SQLBindingItem
{
public:
    SQLBindingItem(UUID uuid_, String pattern_, String serialized_ast_, bool is_regular_expression_, UInt64 timestamp_ = 0)
        : uuid(std::move(uuid_))
        , pattern(std::move(pattern_))
        , serialized_ast(std::move(serialized_ast_))
        , is_regular_expression(is_regular_expression_)
        , timestamp(timestamp_)
    {
    }

    UUID uuid = UUIDHelpers::Nil;
    String pattern;
    String serialized_ast;
    bool is_regular_expression;
    UInt64 timestamp;
};

using SQLBindingItemPtr = std::shared_ptr<SQLBindingItem>;
using SQLBindings = std::vector<SQLBindingItemPtr>;

}
