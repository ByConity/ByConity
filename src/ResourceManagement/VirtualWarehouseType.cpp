#include <ResourceManagement/VirtualWarehouseType.h>

namespace DB::ResourceManagement
{
    bool isSystemVW(const String & virtual_warehouse)
    {
        bool ret = false;
        for (uint8_t i = VirtualWarehouseType::Read; i != VirtualWarehouseType::Num; i++)
        {
            auto vw_type = static_cast<VirtualWarehouseType>(i);
            if (toSystemVWName(vw_type) == virtual_warehouse)
                ret = true;
        }
        return ret;
    }

}
