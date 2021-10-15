#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/ResourceGroupManager.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_GROUP_ILLEGAL_CONFIG;
    extern const int RESOURCE_GROUP_MISMATCH;
}

std::shared_ptr<ResourceSelectCase::QueryType> ResourceSelectCase::translateQueryType(const DB::String &queryType)
{
    if (queryType == "DDL")
        return std::make_shared<ResourceSelectCase::QueryType>(ResourceSelectCase::QueryType::DDL);
    else if (queryType == "DATA")
        return std::make_shared<ResourceSelectCase::QueryType>(ResourceSelectCase::QueryType::DATA);
    else if (queryType == "SELECT")
        return std::make_shared<ResourceSelectCase::QueryType>(ResourceSelectCase::QueryType::SELECT);
    else if (queryType == "OTHER")
        return std::make_shared<ResourceSelectCase::QueryType>(ResourceSelectCase::QueryType::OTHER);
    return nullptr;
}

ResourceSelectCase::QueryType ResourceSelectCase::getQueryType(const DB::IAST *ast)
{
    if (ast->as<ASTCreateQuery>() || ast->as<ASTDropQuery>() || ast->as<ASTRenameQuery>())
        return ResourceSelectCase::QueryType::DDL;
    else if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
        return ResourceSelectCase::QueryType::SELECT;
    else if (ast->as<ASTInsertQuery>())
        return ResourceSelectCase::QueryType::DATA;
    else if (const auto * alter_selects = ast->as<ASTAlterQuery>())
    {
        if (alter_selects->command_list)
        {
            for (auto command : alter_selects->command_list->children)
            {
                auto alter_command = command->as<ASTAlterCommand>();
                if (alter_command->type == ASTAlterCommand::Type::ADD_COLUMN
                  || alter_command->type == ASTAlterCommand::Type::DROP_COLUMN
                  || alter_command->type == ASTAlterCommand::Type::MODIFY_COLUMN
                  || alter_command->type == ASTAlterCommand::Type::COMMENT_COLUMN
                  || alter_command->type == ASTAlterCommand::Type::MODIFY_ORDER_BY
                  || alter_command->type == ASTAlterCommand::Type::MODIFY_TTL
                  || alter_command->type == ASTAlterCommand::Type::ADD_INDEX
                  || alter_command->type == ASTAlterCommand::Type::DROP_INDEX)
                //   || alter_command->type == ASTAlterCommand::Type::CHANGE_ENGINE
                    return ResourceSelectCase::QueryType::DDL;
            }
        }
        return ResourceSelectCase::QueryType::DATA;
    }
    return ResourceSelectCase::QueryType::OTHER;
}

void ResourceGroupManager::loadFromConfig(const Poco::Util::AbstractConfiguration &config)
{
    LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "Load resource group manager");
    if (!rootGroups.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), " does not support reload config, restart needed");
        return;
    }
    Poco::Util::AbstractConfiguration::Keys config_keys;
    String prefix = "resource_groups";
    config.keys(prefix, config_keys);
    String prefixWithKey;
    /// load resource groups
    for (const String &key : config_keys)
    {
        prefixWithKey = prefix + "." + key;
        if (key.find("resource_group") == 0)
        {
            if (!config.has(prefixWithKey + ".name"))
                throw Exception("Resource group has no name", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            String name = config.getString(prefixWithKey + ".name");
            LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "Found resource group {}", name);
            if (groups.find(name) != groups.end())
                throw Exception("Resource group name duplicated: " + name, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            auto pr = groups.emplace(std::piecewise_construct,
                                     std::forward_as_tuple(name),
                                     std::forward_as_tuple());
            InternalResourceGroup *group = &(pr.first->second);
            group->setName(name);
            group->setSoftMaxMemoryUsage(config.getInt64(prefixWithKey + ".soft_max_memory_usage", 0));
            group->setMinQueryMemoryUsage(
                    config.getInt64(prefixWithKey + ".min_query_memory_usage", 536870912)); /// default 512MB
            group->setMaxConcurrentQueries(
                    config.getInt(prefixWithKey + ".max_concurrent_queries", 100)); /// default 100
            group->setMaxQueued(config.getInt(prefixWithKey + ".max_queued", 100)); /// default 100
            group->setMaxQueuedWaitingMs(config.getInt(prefixWithKey + ".max_queued_waiting_ms", 5000)); /// default 5s
            group->setPriority(config.getInt(prefixWithKey + ".priority", 0));
            if (config.has(prefixWithKey + ".parent_resource_group"))
            {
                String parentResourceGroup = config.getString(prefixWithKey + ".parent_resource_group");
                group->setParentResourceGroup(parentResourceGroup);
            }
            else
            {
                /// root groups
                group->setRoot();
                rootGroups.push_back(group);
            }
        }
    }
    /// Set parents
    for (auto & p : groups)
    {
        if (!p.second.getParentResourceGroup().empty())
        {
            auto parentIt = groups.find(p.second.getParentResourceGroup());
            if (parentIt == groups.end())
                throw Exception("Resource group's parent group not found: " + p.second.getName() + " -> " +  p.second.getParentResourceGroup(), ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            p.second.setParent(&(parentIt->second));
        }
    }
    /// load cases
    for (const String & key : config_keys)
    {
        prefixWithKey = prefix + "." + key;
        if (key.find("case") == 0)
        {
            LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "Found resource group case {}", key);
            ResourceSelectCase selectCase;
            if (!config.has(prefixWithKey + ".resource_group"))
                throw Exception("Select case " + key + " does not config resource group", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            selectCase.name = key;
            String resourceGroup = config.getString(prefixWithKey + ".resource_group");
            auto groupIt = groups.find(resourceGroup);
            if (groupIt == groups.end())
                throw Exception("Select case's group not found: " + key + " -> " + resourceGroup, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            else if (!groupIt->second.isLeaf())
                throw Exception("Select case's group is not leaf group: " + key + " -> " + resourceGroup, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            selectCase.group = &(groupIt->second);
            if (config.has(prefixWithKey + ".user"))
                selectCase.user = std::make_shared<std::regex>(config.getString(prefixWithKey + ".user"));
            if (config.has(prefixWithKey + ".query_id"))
                selectCase.queryId = std::make_shared<std::regex>(config.getString(prefixWithKey + ".query_id"));
            if (config.has(prefixWithKey + ".query_type"))
            {
                String queryType = config.getString(prefixWithKey + ".query_type");
                selectCase.queryType = ResourceSelectCase::translateQueryType(queryType);
                if (selectCase.queryType == nullptr)
                    throw Exception("Select case's query type is illegal: " + key + " -> " + queryType, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            }
            selectCases.push_front(std::move(selectCase));
        }
    }
    selectCases.sort([] (const ResourceSelectCase & c1, const ResourceSelectCase & c2) {
        return c1.name < c2.name;
    });
    LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "Found {} resource groups, {} select cases", groups.size(), selectCases.size());
    bool oldVal = false;
    if (!rootGroups.empty() && started.compare_exchange_strong(oldVal, true, std::memory_order_seq_cst, std::memory_order_relaxed))
    {
        ResourceTask *resourceTask = new ResourceTask(this);
        timer.scheduleAtFixedRate(resourceTask, 1, 1);
    }
}

void ResourceGroupManager::enable()
{
    disabled.store(false, std::memory_order_relaxed);
    LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "enabled");
}

void ResourceGroupManager::disable()
{
    disabled.store(true, std::memory_order_relaxed);
    LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "disabled");
}

InternalResourceGroup* ResourceGroupManager::selectGroup(const Context &query_context, const IAST *ast)
{
    const ClientInfo & client_info = query_context.getClientInfo();
    for (const auto & selectCase : selectCases)
    {
        if ((selectCase.user == nullptr || std::regex_match(client_info.initial_user, *(selectCase.user)))
            && (selectCase.queryId == nullptr || std::regex_match(client_info.initial_query_id, *(selectCase.queryId)))
            && (selectCase.queryType == nullptr || *selectCase.queryType == ResourceSelectCase::getQueryType(ast)))
            return selectCase.group;
    }
    switch(query_context.getSettingsRef().resource_group_unmatched_behavior)
    {
        case 0:
            return nullptr;
        case 1:
            throw Exception("Match no existing resource group", ErrorCodes::RESOURCE_GROUP_MISMATCH);
        case 2:
            if (!rootGroups.empty())
                return rootGroups.front();
            else
                throw Exception("Match no existing resource group", ErrorCodes::RESOURCE_GROUP_MISMATCH);
        default:
            throw Exception("Invalid resource_group_unmatched_behavior value", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
    }
}

ResourceGroupManager::~ResourceGroupManager() = default;

ResourceGroupManager::Info ResourceGroupManager::getInfo() const
{
    ResourceGroupManager::Info infos;
    infos.reserve(groups.size());
    for (const auto & pr : groups)
    {
        infos.emplace_back(pr.second.getInfo());
    }
    return infos;
}

}
