#include <Protos/PreparedStatementHelper.h>

namespace DB
{
void setSizeOrVariableToProto(const SizeOrVariable & size_or_var, Protos::SizeOrVariable & proto)
{
    if (const auto * size = std::get_if<size_t>(&size_or_var))
        proto.set_size(*size);
    else
        proto.set_variable(std::get<String>(size_or_var));
}

std::optional<SizeOrVariable> getSizeOrVariableFromProto(const Protos::SizeOrVariable & proto)
{
    if (proto.has_size())
        return SizeOrVariable{proto.size()};
    else if (proto.has_variable())
        return SizeOrVariable{proto.variable()};
    else
        return std::nullopt;
}
}
