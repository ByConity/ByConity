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


#include <optional>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterAlterWarehouseQuery.h>
#include <Interpreters/VirtualWarehouseQueue.h>
#include <Parsers/ASTAlterWarehouseQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Protos/resource_manager_rpc.pb.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include <common/logger_useful.h>
#include "Core/SettingsEnums.h"
#include "Core/SettingsFields.h"
#include "Parsers/ASTFunction.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int RESOURCE_MANAGER_ERROR;
    extern const int RESOURCE_MANAGER_INCOMPATIBLE_SETTINGS;
    extern const int RESOURCE_MANAGER_ILLEGAL_CONFIG;
    extern const int RESOURCE_MANAGER_UNKNOWN_SETTING;
    extern const int RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO;
}

InterpreterAlterWarehouseQuery::InterpreterAlterWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_)
{
}

void checkQueueName(const String & queue_name_)
{
    SettingFieldEnum<QueueName, SettingFieldQueueNameTraits> current_name;
    current_name.parseFromString(queue_name_);
    
    if (current_name.value >= QueueName::Count)
    {
        throw Exception("Unknown queue_name : " + queue_name_, ErrorCodes::SYNTAX_ERROR);
    }
}

BlockIO InterpreterAlterWarehouseQuery::execute()
{
    auto & alter = query_ptr->as<ASTAlterWarehouseQuery &>();
    auto & vw_name = alter.name;

    if (!alter.rename_to.empty())
        throw Exception("Renaming VirtualWarehouse is currently unsupported.", ErrorCodes::LOGICAL_ERROR);

    ResourceManagement::VirtualWarehouseAlterSettings vw_alter_settings;

    // update queue info
    // alter warehouse vw_name add rule set tables = ['xxx', 'yyy'], rule_name = 'rule_xx' where queue_name = 'yyy';
    if (alter.type == ASTAlterWarehouseQuery::Type::ADD_RULE)
    {
        bool has_rule_name = false;
        bool has_queue_name = false;
        vw_alter_settings.queue_data = std::make_optional<DB::ResourceManagement::QueueData>();
        vw_alter_settings.queue_data->queue_rules.resize(1);
        auto & first_rule = vw_alter_settings.queue_data->queue_rules[0];
        vw_alter_settings.queue_alter_type = Protos::QueueAlterType::ADD_RULE;
        for (const auto & child : alter.assignment_list->children)
        {
            if (const ASTAssignment * assignment = child->as<ASTAssignment>())
            {
                const auto & assign_name = assignment->column_name;
                LOG_TRACE(getLogger("InterpreterAlterWarehouseQuery"), "assign name {}", assign_name);
                if (const ASTLiteral * literal = assignment->expression()->as<ASTLiteral>())
                {
                    if (assign_name == "rule_name")
                    {
                        has_rule_name = true;
                        first_rule.rule_name = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "tables" && (literal->value.getType() == Field::Types::Array))
                    {
                        for (const auto & table : literal->value.safeGet<Array>())
                        {
                            first_rule.tables.push_back(table.safeGet<String>());
                        }
                    }
                    else if (assign_name == "databases" && (literal->value.getType() == Field::Types::Array))
                    {
                        for (const auto & database : literal->value.safeGet<Array>())
                        {
                            first_rule.databases.push_back(database.safeGet<String>());
                        }
                    }
                    else if (assign_name == "ip")
                    {
                        first_rule.ip = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "user")
                    {
                        first_rule.user = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "query_id")
                    {
                        first_rule.query_id = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "fingerprint")
                    {
                        first_rule.fingerprint = literal->value.safeGet<String>();
                    }
                    else
                        throw Exception("Syntax error in alter warehouse statement. " + child->getID(), ErrorCodes::SYNTAX_ERROR);
                }
                else
                    throw Exception("Syntax error in alter warehouse statement. " + child->getID(), ErrorCodes::SYNTAX_ERROR);
            }
            else
                throw Exception("Syntax error in alter warehouse statement. " + child->getID(), ErrorCodes::SYNTAX_ERROR);
        }

        std::map<String, String> column_to_value;
        collectWhereClausePredicate(alter.predicate, column_to_value);
        if (column_to_value.find("queue_name") != column_to_value.end())
        {
            has_queue_name = true;
            checkQueueName(column_to_value["queue_name"]);
            vw_alter_settings.queue_data->queue_name = column_to_value["queue_name"];
        }

        if (!has_rule_name || !has_queue_name)
            throw Exception("Syntax error : Not contain `rule_name` or `queue_name`", ErrorCodes::SYNTAX_ERROR);

        if (auto client = getContext()->getResourceManagerClient())
        {
            client->updateVirtualWarehouse(vw_name, vw_alter_settings);
        }
        else
            throw Exception("Can't apply DDL of warehouse as RM is not enabled.", ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }
    // alter warehouse vw_name delete rule where rule_name = 'rule_xx' and queue_name = 'yyy';
    else if (alter.type == ASTAlterWarehouseQuery::Type::DELETE_RULE)
    {
        bool has_rule_name = false;
        bool has_queue_name = false;
        vw_alter_settings.queue_alter_type = Protos::QueueAlterType::DELETE_RULE;
        std::map<String, String> column_to_value;
        collectWhereClausePredicate(alter.predicate, column_to_value);
        if (column_to_value.find("queue_name") != column_to_value.end())
        {
            has_queue_name = true;
            checkQueueName(column_to_value["queue_name"]);
            vw_alter_settings.queue_name = column_to_value["queue_name"];
        }
        if (column_to_value.find("rule_name") != column_to_value.end())
        {
            has_rule_name = true;
            vw_alter_settings.rule_name = column_to_value["rule_name"];
        }
        if (!has_rule_name || !has_queue_name)
            throw Exception("Syntax error : Not contain `rule_name` or `queue_name`", ErrorCodes::SYNTAX_ERROR);

        if (auto client = getContext()->getResourceManagerClient())
        {
            client->updateVirtualWarehouse(vw_name, vw_alter_settings);
        }
        else
            throw Exception("Can't apply DDL of warehouse as RM is not enabled.", ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }
    // alter warehouse vw_name modify rule set max_concurrency = 3 where queue_name = 'yyy';
    else if (alter.type == ASTAlterWarehouseQuery::Type::MODIFY_RULE)
    {
        vw_alter_settings.queue_alter_type = Protos::QueueAlterType::MODIFY_RULE;

        for (const auto & child : alter.assignment_list->children)
        {
            if (const ASTAssignment * assignment = child->as<ASTAssignment>())
            {
                const auto & assign_name = assignment->column_name;
                LOG_TRACE(
                    getLogger("InterpreterAlterWarehouseQuery"),
                    "assign name {}, assygn type {}",
                    assign_name,
                    int(assignment->expression()->getType()));
                if (const ASTLiteral * literal = assignment->expression()->as<ASTLiteral>())
                {
                    if (assign_name == "max_concurrency")
                    {
                        vw_alter_settings.max_concurrency = literal->value.safeGet<size_t>();
                    }
                    else if (assign_name == "query_queue_size")
                    {
                        vw_alter_settings.query_queue_size = literal->value.safeGet<size_t>();
                    }
                    else if (assign_name == "query_id")
                    {
                        vw_alter_settings.query_id = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "user")
                    {
                        vw_alter_settings.user = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "ip")
                    {
                        vw_alter_settings.ip = literal->value.safeGet<String>();
                    }
                    else if (assign_name == "tables")
                    {
                        vw_alter_settings.has_table = true;
                        auto tables = literal->value.safeGet<Array>();
                        for (const auto & table : tables)
                        {
                            vw_alter_settings.tables.push_back(table.safeGet<String>());
                        }
                    }
                    else if (assign_name == "databases")
                    {
                        vw_alter_settings.has_database = true;
                        auto dbs = literal->value.safeGet<Array>();
                        for (const auto & db : dbs)
                        {
                            vw_alter_settings.databases.push_back(db.safeGet<String>());
                        }
                    }
                    else if (assign_name == "fingerprint")
                    {
                        vw_alter_settings.fingerprint = literal->value.safeGet<String>();
                    }
                    else
                        throw Exception("Unknown assign name " + assign_name, ErrorCodes::SYNTAX_ERROR);
                }
                else if (const ASTFunction * func = assignment->expression()->as<ASTFunction>())
                {
                    // In sql : alter warehouse  vw1 MODIFY RULE set tables = []  where queue_name = 'q1' and rule_name = 'r1';
                    // The type of [] is array function.
                    if (func->name == "array")
                    {
                        if (assign_name == "tables")
                            vw_alter_settings.has_table = true;
                        else if (assign_name == "databases")
                            vw_alter_settings.has_database = true;
                    }
                }
                else
                {
                    throw Exception("Unknown type", ErrorCodes::SYNTAX_ERROR);
                }
            }
        }
        std::map<String, String> column_to_value;
        collectWhereClausePredicate(alter.predicate, column_to_value);
        if (column_to_value.find("queue_name") != column_to_value.end())
        {
            checkQueueName(column_to_value["queue_name"]);
            vw_alter_settings.queue_name = column_to_value["queue_name"];
        }
        else
        {
            throw Exception("Syntax error : Not contains `queue_name`", ErrorCodes::SYNTAX_ERROR);
        }
        if (column_to_value.find("rule_name") != column_to_value.end())
        {
            vw_alter_settings.rule_name = column_to_value["rule_name"];
        }
        if (auto client = getContext()->getResourceManagerClient())
        {
            client->updateVirtualWarehouse(vw_name, vw_alter_settings);
        }
        else
            throw Exception("Can't apply DDL of warehouse as RM is not enabled.", ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }
    else
    {
        if (alter.settings)
        {
            for (const auto & change : alter.settings->changes)
            {
                if (change.name == "type")
                {
                    using RMType = ResourceManagement::VirtualWarehouseType;
                    auto value = change.value.safeGet<std::string>();
                    auto type = RMType(ResourceManagement::toVirtualWarehouseType(&value[0]));
                    if (type == RMType::Unknown)
                        throw Exception("Unknown Virtual Warehouse type: " + value, ErrorCodes::RESOURCE_MANAGER_UNKNOWN_SETTING);
                    vw_alter_settings.type = type;
                }
                else if (change.name == "auto_suspend")
                {
                    vw_alter_settings.auto_suspend = change.value.safeGet<size_t>();
                }
                else if (change.name == "auto_resume")
                {
                    vw_alter_settings.auto_resume = change.value.safeGet<size_t>();
                }
                else if (change.name == "num_workers")
                {
                    vw_alter_settings.num_workers = change.value.safeGet<size_t>();
                }
                else if (change.name == "min_worker_groups")
                {
                    vw_alter_settings.min_worker_groups = change.value.safeGet<size_t>();
                }
                else if (change.name == "max_worker_groups")
                {
                    vw_alter_settings.max_worker_groups = change.value.safeGet<size_t>();
                }
                else if (change.name == "max_concurrent_queries")
                {
                    vw_alter_settings.max_concurrent_queries = change.value.safeGet<size_t>();
                }
                else if (change.name == "max_queued_queries")
                {
                    vw_alter_settings.max_queued_queries = change.value.safeGet<size_t>();
                }
                else if (change.name == "max_queued_waiting_ms")
                {
                    vw_alter_settings.max_queued_waiting_ms = change.value.safeGet<size_t>();
                }
                else if (change.name == "vw_schedule_algo")
                {
                    auto value = change.value.safeGet<std::string>();
                    auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
                    if (algo == ResourceManagement::VWScheduleAlgo::Unknown)
                        throw Exception("Wrong vw_schedule_algo: " + value, ErrorCodes::RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO);
                    vw_alter_settings.vw_schedule_algo = algo;
                }
                else if (change.name == "max_auto_borrow_links")
                {
                    vw_alter_settings.max_auto_borrow_links = change.value.safeGet<size_t>();
                }
                else if (change.name == "max_auto_lend_links")
                {
                    vw_alter_settings.max_auto_lend_links = change.value.safeGet<size_t>();
                }
                else if (change.name == "cpu_busy_threshold")
                {
                    vw_alter_settings.cpu_busy_threshold = change.value.safeGet<size_t>();
                }
                else if (change.name == "mem_busy_threshold")
                {
                    vw_alter_settings.mem_busy_threshold = change.value.safeGet<size_t>();
                }
                else if (change.name == "cpu_idle_threshold")
                {
                    vw_alter_settings.cpu_idle_threshold = change.value.safeGet<size_t>();
                }
                else if (change.name == "mem_idle_threshold")
                {
                    vw_alter_settings.mem_idle_threshold = change.value.safeGet<size_t>();
                }
                else if (change.name == "cpu_threshold_for_recall")
                {
                    vw_alter_settings.cpu_threshold_for_recall = change.value.safeGet<size_t>();
                }
                else if (change.name == "mem_threshold_for_recall")
                {
                    vw_alter_settings.mem_threshold_for_recall = change.value.safeGet<size_t>();
                }
                else if (change.name == "cooldown_seconds_after_scaleup")
                {
                    vw_alter_settings.cooldown_seconds_after_scaleup = change.value.safeGet<size_t>();
                }
                else if (change.name == "cooldown_seconds_after_scaledown")
                {
                    vw_alter_settings.cooldown_seconds_after_scaledown = change.value.safeGet<size_t>();
                }
                else
                {
                    throw Exception("Unknown setting " + change.name, ErrorCodes::RESOURCE_MANAGER_UNKNOWN_SETTING);
                }
            }
        }

        if (vw_alter_settings.min_worker_groups && vw_alter_settings.max_worker_groups
            && vw_alter_settings.min_worker_groups > vw_alter_settings.max_worker_groups)
            throw Exception(
                "min_worker_groups should be less than or equal to max_worker_groups", ErrorCodes::RESOURCE_MANAGER_INCOMPATIBLE_SETTINGS);

        if (auto client = getContext()->getResourceManagerClient())
            client->updateVirtualWarehouse(vw_name, vw_alter_settings);
        else
            throw Exception("Can't apply DDL of warehouse as RM is not enabled.", ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }

    return {};
}
}
