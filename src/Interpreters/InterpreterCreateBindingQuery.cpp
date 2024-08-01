#include <Catalog/Catalog.h>

#include <Access/ContextAccess.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/InterpreterCreateBindingQuery.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Interpreters/SQLBinding/SQLBindingCatalog.h>
#include <Interpreters/SQLBinding/SQLBindingUtils.h>
#include <Parsers/ASTSQLBinding.h>
#include <Parsers/ASTSerDerHelper.h>

namespace DB
{

BlockIO InterpreterCreateBindingQuery::execute()
{
    const auto * create = query_ptr->as<const ASTCreateBinding>();
    if (!create)
        throw Exception("Create SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);

    if (create->level == BindingLevel::GLOBAL)
    {
        AccessRightsElements access_rights_elements;
        access_rights_elements.emplace_back(AccessType::CREATE_BINDING);

        if (create->or_replace)
            access_rights_elements.emplace_back(AccessType::DROP_BINDING);

        context->checkAccess(access_rights_elements);
    }

    const auto & tenant_id = context->getTenantId();

    WriteBufferFromOwnString ast_buffer;
    UUID pattern_uuid;
    SQLBindingItemPtr binding;
    if (!create->re_expression.empty() && create->getSettings())
    {
        // get re_expression UUID
        pattern_uuid = SQLBindingUtils::getReExpressionHash(
            create->re_expression.data(), create->re_expression.data() + create->re_expression.size(), tenant_id);
        // get re_expression object
        auto re_ptr = std::make_shared<boost::regex>(create->re_expression);
        // If binding level is SESSION, we will write it to cache, if GLOBAL, we will write it to the catalog and cache。
        if (create->level == BindingLevel::SESSION)
        {
            auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
            session_binding_cache_manager->addReBinding(
                pattern_uuid, {create->re_expression, tenant_id, nullptr, create->getSettings()->clone(), re_ptr}, !create->if_not_exists, create->or_replace);
        }
        else
        {
            auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
            serializeAST(create->getSettings(), ast_buffer);
            UInt64 time_stamp
                = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            binding = std::make_shared<SQLBindingItem>(pattern_uuid, create->re_expression, ast_buffer.str(), true, time_stamp, tenant_id);
            // update cache
            global_binding_cache_manager->addReBinding(
                pattern_uuid, {create->re_expression, tenant_id, nullptr, create->getSettings()->clone(), re_ptr}, !create->if_not_exists, create->or_replace);
        }
    }
    else if (!create->query_pattern.empty() && create->getTarget())
    {
        pattern_uuid = SQLBindingUtils::getQueryASTHash(create->getPattern(), tenant_id);
        if (create->getPattern() && pattern_uuid == UUIDHelpers::Nil)
            throw Exception("Get binding pattern uuid failed", ErrorCodes::LOGICAL_ERROR);
        if (create->level == BindingLevel::SESSION)
        {
            auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
            session_binding_cache_manager->addSqlBinding(
                pattern_uuid, {create->query_pattern, tenant_id, create->getTarget()->clone(), nullptr, nullptr}, !create->if_not_exists, create->or_replace);
        }
        else
        {
            auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
            serializeAST(create->getTarget(), ast_buffer);
            binding = std::make_shared<SQLBindingItem>(pattern_uuid, create->query_pattern, ast_buffer.str(), false, 0, tenant_id);
            // update cache
            global_binding_cache_manager->addSqlBinding(pattern_uuid, {create->query_pattern, tenant_id, create->getTarget()->clone(), nullptr, nullptr}, !create->if_not_exists, create->or_replace);
        }
    }
    else
        throw Exception("Create SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);

    String output = "Create SQL Binding succeeded";
    // Update the cache of all servers
    if (create->level == BindingLevel::GLOBAL)
    {
        BindingCatalogManager catalog(context);
        catalog.updateSQLBinding(binding);
        try
        {
            catalog.updateGlobalBindingCache(context);
        }
        catch (...)
        {
            output = "Create SQL Binding succeeded, But some nodes failed to refresh sql binding cache.";
            LOG_ERROR(log, output);
        }
    }

    BlockIO res;
    Block block;
    ColumnWithTypeAndName col;
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);
    MutableColumns res_columns = block.cloneEmptyColumns();
    res_columns[0]->insertData(output.data(), output.size());
    res.in = std::make_shared<OneBlockInputStream>(block.cloneWithColumns(std::move(res_columns)));
    return res;
}
}
