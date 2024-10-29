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

#include <Common/Configurations.h>
#include <Common/Config/ConfigProcessor.h>

namespace DB
{

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

void RootConfiguration::loadFromPocoConfigImpl(const PocoAbstractConfig & config, const String &)
{
    // resource_manager.loadFromPocoConfig(config, "rm_service");
    resource_manager.loadFromPocoConfig(config, "resource_manager");
    service_discovery.loadFromPocoConfig(config, "service_discovery");
    service_discovery_kv.loadFromPocoConfig(config, "service_discovery_kv");
    queue_manager.loadFromPocoConfig(config, "queue_manager");
    adaptive_scheduler.loadFromPocoConfig(config, "adaptive_scheduler");
    tso_service.loadFromPocoConfig(config, "tso_service");
    bulk_synchronous_parallel.loadFromPocoConfig(config, "bulk_synchronous_parallel");
}

}
