#pragma once

#include <string>
namespace DB::Catalog
{
struct CatalogConfig
{
    struct ByteKV
    {
        std::string service_name;
        std::string cluster_name;
        std::string name_space;
        std::string table_name;
    };

    ByteKV byteKV;
};

}
