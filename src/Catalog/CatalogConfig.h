/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int METASTORE_EXCEPTION;
} // namespace DB::ErrorCodes

namespace DB::Catalog
{

enum class StoreType
{
    UNINIT,
    BYTEKV,
    FDB
};

struct CatalogConfig
{
    struct FDBConf
    {
        std::string cluster_conf_path;
    };

    struct ByteKVConf
    {
        std::string service_name;
        std::string cluster_name;
        std::string name_space;
        std::string table_name;
    };

    CatalogConfig() {}

    CatalogConfig(const Poco::Util::AbstractConfiguration & poco_config, const std::string& prefix = "catalog_service")
    {

        if (poco_config.has(prefix))
        {
            std::string key_catalog_type = prefix + ".type";
            if (poco_config.has(key_catalog_type))
            {
                std::string metastore_type = poco_config.getString(key_catalog_type);
                if (metastore_type == "fdb")
                    type = StoreType::FDB;
                else if (metastore_type == "bytekv")
                    type = StoreType::BYTEKV;
                else
                    throw Exception("Unsupportted metastore type " + metastore_type, ErrorCodes::METASTORE_EXCEPTION);
            }

            if (type == StoreType::FDB)
            {
                std::string key_fdb_cluster_file = prefix + ".fdb.cluster_file";
                fdb_conf.cluster_conf_path = poco_config.getString(key_fdb_cluster_file);
            }
            else if (type == StoreType::BYTEKV)
            {
                std::string key_bytekv_service_name = prefix + ".bytekv.service_name";
                std::string key_bytekv_cluster_name = prefix + "bytekv.cluster_name";
                std::string key_bytekv_namespace = prefix + "bytekv.name_space";
                std::string key_bytekv_table_name = prefix + "bytekv.table_name";
                bytekv_conf.service_name = poco_config.getString(key_bytekv_service_name);
                bytekv_conf.cluster_name = poco_config.getString(key_bytekv_cluster_name);
                bytekv_conf.name_space = poco_config.getString(key_bytekv_namespace);
                bytekv_conf.table_name = poco_config.getString(key_bytekv_table_name);
            }
        }
    }

    StoreType type = StoreType::UNINIT;
    FDBConf fdb_conf;
    ByteKVConf bytekv_conf;
};

}
