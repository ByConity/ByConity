#include <vector>
#include <optional>
#include <ResourceGroup/ResourceGroupSelectStrategy.h>
#include <ResourceGroup/IResourceGroupManager.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/thread_local_rng.h>
#include <Parsers/ASTSelectQuery.h>
#include <common/logger_useful.h>
#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_GROUP_ILLEGAL_CONFIG;
    extern const int RESOURCE_GROUP_MISMATCH;
}

IResourceGroup* UserTableSelectStrategy::selectGroup(const Context & query_context, const IAST *ast)
{
    auto & select_cases = resource_group_manager->getSelectCases();
    const ClientInfo & client_info = query_context.getClientInfo();

    const auto get_user_group = [&select_cases, &client_info]() -> IResourceGroup * {
        for (const auto & [_, select_case] : select_cases)
        {
            if ((select_case.user == nullptr || std::regex_match(client_info.initial_user, *(select_case.user))))
                return select_case.group;
        }
        return nullptr;
    };

    IResourceGroup * user_group = get_user_group();
    LOG_DEBUG(logger, "user {} user_group {} children_size {}", 
            client_info.initial_user, user_group ? user_group->getName() : "not found", 
            user_group ? user_group->getChildren().size() : 0);
    if (user_group && !user_group->getChildren().empty())
    {
        size_t group_idx = 0;

        std::vector<IResourceGroup *> groups;
        const auto & children = user_group->getChildren();
        for (const auto & item: children)
        {
            groups.emplace_back(item.second);
        }

        const ASTSelectQuery * select_query = nullptr;
        if (ast->as<ASTSelectQuery>())
        {
            select_query = ast->as<ASTSelectQuery>();
        } 
        else if (ast->as<ASTSelectWithUnionQuery>())
        {
            const ASTSelectWithUnionQuery * union_ast = ast->as<ASTSelectWithUnionQuery>();
            if (union_ast->list_of_selects->children.size() == 1)
            {
                const ASTPtr first_select_ast = union_ast->list_of_selects->children.at(0);
                select_query = dynamic_cast<ASTSelectQuery *>(first_select_ast.get());
            }
        }

        std::optional<DatabaseAndTableWithAlias> database_and_table;
        if (select_query)
        {
            database_and_table = getDatabaseAndTable(*select_query, 0);
        }

        LOG_DEBUG(logger, "user_query {} user_group {} table {}", 
            select_query ? "select" : "not_select", 
            user_group->getName(), 
            database_and_table ? database_and_table->table : "empty");

        if (select_query && database_and_table.has_value())
        {
            auto table = database_and_table->table;
            if (query_context.getServerType() == ServerType::cnch_worker)
            {
                table = table.substr(0, table.find_last_of('_'));
            }

            group_idx = CityHash_v1_0_2::CityHash64(table.data(), table.length()) % children.size();
            LOG_DEBUG(logger, "user {} table {} group_idx {}", client_info.initial_user, table, group_idx);
        }
        else
        {
            std::uniform_int_distribution dist;
            group_idx = dist(thread_local_rng) % children.size();
        }
        return groups[group_idx];
    }

    const auto & root_groups = resource_group_manager->getRootGroups();
    switch (query_context.getSettingsRef().resource_group_unmatched_behavior)
    {
        case 0:
            return nullptr;
        case 1:
            throw Exception("Match no existing resource group", ErrorCodes::RESOURCE_GROUP_MISMATCH);
        case 2:
            if (!root_groups.empty())
                return root_groups.begin()->second;
            else
                throw Exception("Match no existing resource group", ErrorCodes::RESOURCE_GROUP_MISMATCH);
        default:
            throw Exception("Invalid resource_group_unmatched_behavior value", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
    }
}

IResourceGroup* UserQuerySelectStrategy::selectGroup(const Context & query_context, const IAST *ast)
{
    auto & select_cases = resource_group_manager->getSelectCases();

    const ClientInfo & client_info = query_context.getClientInfo();
    IResourceGroup * res_group = nullptr;
    for (const auto & [_, select_case] : select_cases)
    {
        if ((select_case.user == nullptr || std::regex_match(client_info.initial_user, *(select_case.user)))
            && (select_case.query_id == nullptr || std::regex_match(client_info.initial_query_id, *(select_case.query_id)))
            && (select_case.query_type == nullptr || *select_case.query_type == ResourceSelectCase::getQueryType(ast)))
        {
            res_group = select_case.group;
        }
    }

    LOG_DEBUG(logger, "user {} res_group {} children_size {}", 
            client_info.initial_user, res_group ? res_group->getName() : "not found", 
            res_group ? res_group->getChildren().size() : 0);

    if (res_group)
        return res_group;

    const auto & root_groups = resource_group_manager->getRootGroups();
    switch (query_context.getSettingsRef().resource_group_unmatched_behavior)
    {
        case 0:
            return nullptr;
        case 1:
            throw Exception("Match no existing resource group", ErrorCodes::RESOURCE_GROUP_MISMATCH);
        case 2:
            if (!root_groups.empty())
                return root_groups.begin()->second;
            else
                throw Exception("Match no existing resource group", ErrorCodes::RESOURCE_GROUP_MISMATCH);
        default:
            throw Exception("Invalid resource_group_unmatched_behavior value", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
    }
}


IResourceGroup* VWSelectStrategy::selectGroup(const Context & query_context, const IAST *ast)
{
    auto lock = resource_group_manager->getReadLock();

    const ClientInfo & client_info = query_context.getClientInfo();
    if (auto vw = query_context.tryGetCurrentVW(); vw)
    {
        const String & vw_name = vw->getName();

        auto & select_cases = resource_group_manager->getSelectCases();
        //TODO: Optimize using UFDS when sub-groups are supported
        for (const auto & [key, select_case] : select_cases)
        {
            String parent_resource_group = select_case.group->getName();

            std::vector<IResourceGroup*> parent_groups {select_case.group};
            auto parent = select_case.group->getParent();
            while (parent)
            {
                parent_groups.push_back(parent);
                parent_resource_group = parent->getName();
                parent = parent->getParent();
            }

            if (parent_resource_group == vw_name
                && (select_case.user == nullptr || std::regex_match(client_info.initial_user, *(select_case.user)))
                && (select_case.query_id == nullptr || std::regex_match(client_info.initial_query_id, *(select_case.query_id)))
                && (select_case.query_type == nullptr || *select_case.query_type == ResourceSelectCase::getQueryType(ast)))
            {
                for (const auto & group : parent_groups)
                {
                    group->setInUse(true);
                }
                return select_case.group;
            }
        }
    }

    return nullptr;
}

}
