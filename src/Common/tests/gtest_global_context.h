/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <filesystem>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/time.h>

// May update the config if need.
static std::string default_config = R"#(<?xml version="1.0"?>
<yandex>
    <storage_configuration>
    <disks>
        <local_disk>
            <path>/tmp/.test</path>
        </local_disk>
        <hdfs_disk>
            <path>/hdfs_path/</path>
            <type>hdfs</type>
        </hdfs_disk>
    </disks>
    <policies>
        <default>
            <volumes>
                <local>
                    <default>local_disk</default>
                    <disk>local_disk</disk>
                </local>
            </volumes>
        </default>
        <cnch_default_hdfs>
            <volumes>
                <hdfs>
                    <default>hdfs_disk</default>
                    <disk>hdfs_disk</disk>
                </hdfs>
                <local>
                    <default>local_disk</default>
                    <disk>local_disk</disk>
                </local>
            </volumes>
        </cnch_default_hdfs>
    </policies>
    </storage_configuration>
    <hdfs_addr>hdfs://test</hdfs_addr>
</yandex>)#";

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder()
        : shared_context(DB::Context::createShared())
        , context(DB::Context::createGlobal(shared_context.get()))
    {
        context->makeGlobalContext();
        context->setPath("./");

        // Load default config.
        std::stringstream ss(default_config);
        Poco::XML::InputSource input_source{ss};
        auto * config = new Poco::Util::XMLConfiguration{&input_source};
        context->setConfig(config);

        DB::HDFSConnectionParams hdfs_params = DB::HDFSConnectionParams::parseHdfsFromConfig(context->getConfigRef());
        context->setHdfsConnectionParams(hdfs_params);

        // Register path.
        std::string path = "gtest_tmp/";
        context->setPath(path);
        {
            std::filesystem::create_directories(std::filesystem::path(path) / "disks/");
        }

        DB::DatabasePtr database = std::make_shared<DB::DatabaseMemory>("test_database", context);
        DB::DatabaseCatalog::instance().attachDatabase("test_database", database);
        DB::ConfigurationPtr config_ptr = new Poco::Util::MapConfiguration;
        context->setConfig(config_ptr);
    }

    void resetStoragePolicy() const {
        std::stringstream ss(default_config);
        Poco::XML::InputSource input_source{ss};
        auto * config = new Poco::Util::XMLConfiguration{&input_source};
        context->setConfig(config);
        context->updateStorageConfiguration(*config);
    }

    ContextHolder(ContextHolder &&) = default;
    DB::ContextMutablePtr
    createQueryContext(const String & query_id, const std::unordered_map<std::string, DB::Field> & settings = {}) const;
};

inline const ContextHolder & getContext()
{
    static ContextHolder holder;
    return holder;
}

inline void setQueryDuration(DB::ContextMutablePtr context = nullptr)
{
    if (!context)
        context = getContext().context;

    auto & client_info = context->getClientInfo();

    const auto current_time = std::chrono::system_clock::now();
    client_info.initial_query_start_time = time_in_seconds(current_time);
    client_info.initial_query_start_time_microseconds = time_in_microseconds(current_time);

    context->initQueryExpirationTimeStamp();
}
