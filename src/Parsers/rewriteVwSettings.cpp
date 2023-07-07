#include <Core/Field.h>
#include <Parsers/rewriteVwSettings.h>
#include <Interpreters/Context.h>

namespace DB
{

//"vw='{vw}'"=> "virtual_warehouse='vw-{tenant_id}-{vw}'" and replace all the '_' into '-'.
bool tryRewriteVwSettings(String &name, Field &value, IParser::Pos & pos)
{
    const auto *context = pos.getContext();
    if (context && (!context->getTenantId().empty()) && value.getType() == Field::Types::String && name == "vw")
    {
        name = "virtual_warehouse";
        String new_value = "vw-";
        new_value += context->getTenantId();
        new_value += '-';
        String old_value;
        value.tryGet(old_value);
        std::replace(old_value.begin(), old_value.end(), '_', '-');
        new_value += old_value;
        value = std::move(new_value);
        return true;
    }
    return false;
}

}



