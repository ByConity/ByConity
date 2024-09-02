#include <Core/Field.h>
#include <Parsers/rewriteVwSettings.h>
#include <Interpreters/Context.h>

namespace DB
{

//"vw='{vw}'"=> "virtual_warehouse='vw-{tenant_id}-{vw}'" and replace all the '_' into '-'.
bool tryRewriteVwSettings(String &name, Field &value, IParser::Pos & pos)
{
    const auto *context = pos.getContext();
    if (context && (!context->getTenantId().empty()) && value.getType() == Field::Types::String)
    {
        if (name == "vw")
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
        else if (name == "virtual_warehouse" || name == "cnch_vw_default" || name == "cnch_vw_read" || name == "cnch_vw_write" || name == "cnch_vw_task")
        {
            // if virtual_warehouse does not match the structure i.e. --> 'vw-{tenant_id}-{vw}', then we return true and move on
            String current_tenant_id = context->getTenantId();
            String vw_string;
            value.tryGet(vw_string);
            String tenant_id_from_vw = getTenantIdFromVWString(vw_string);
            if (!tenant_id_from_vw.empty() && tenant_id_from_vw != current_tenant_id)
                throw Exception("This user cannot access this virtual_warehouse belonging to tenant_id = " + tenant_id_from_vw, ErrorCodes::BAD_ARGUMENTS);
            return true;
        }
    }
    return false;
}

// if it matches structure, return the tenant_id
// otherwise returns empty tenant_id to signify that the virtual_warehouse field does not match the required structure
String getTenantIdFromVWString(String virtual_warehouse, String delimiter)
{
    size_t pos = 0;
    size_t i = 0;
    String token;
    String tenant_id;
    while ((pos = virtual_warehouse.find(delimiter)) != String::npos) {
        token = virtual_warehouse.substr(0, pos);
        if (i == 0 && token != "vw")
            break;
        if (i == 1)
            tenant_id = token;
        virtual_warehouse.erase(0, pos + delimiter.length());
        i++;
    }
    return tenant_id;
}

}



