#pragma once
#include <Interpreters/prepared_statement.h>
#include <Protos/plan_node_utils.pb.h>

#include <optional>

namespace DB
{
void setSizeOrVariableToProto(const SizeOrVariable & size_or_var, Protos::SizeOrVariable & proto);
std::optional<SizeOrVariable> getSizeOrVariableFromProto(const Protos::SizeOrVariable & proto);
}
