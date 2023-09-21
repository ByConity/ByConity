#include <Catalog/Catalog.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/InterpreterCreateBinding.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Interpreters/SQLBinding/SQLBindingCatalog.h>
#include <Interpreters/SQLBinding/SQLBindingUtils.h>
#include <Parsers/ASTSQLBinding.h>
#include <Parsers/ASTSerDerHelper.h>

namespace DB
{

BlockIO InterpreterCreateBinding::execute()
{
    const auto * create = query_ptr->as<const ASTCreateBinding>();
    if (!create)
        throw Exception("Create SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);

    WriteBufferFromOwnString ast_buffer;
    UUID pattern_uuid;
    SQLBindingItemPtr binding;
    if (!create->re_expression.empty() && create->settings())
    {
        // get re_expression UUID
        pattern_uuid = SQLBindingUtils::getReExpressionHash(
            create->re_expression.data(), create->re_expression.data() + create->re_expression.size());
        // get re_expression object
        auto re_ptr = std::make_shared<boost::regex>(create->re_expression);
        // If binding level is SESSION, we will write it to cache, if GLOBAL, we will write it to the catalog and cache。
        if (create->level == BindingLevel::SESSION)
        {
            auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
            session_binding_cache_manager->addReBinding(
                pattern_uuid, {create->re_expression, nullptr, create->settings()->clone(), re_ptr});
        }
        else
        {
            auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
            serializeAST(create->settings(), ast_buffer);
            UInt64 time_stamp
                = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            binding = std::make_shared<SQLBindingItem>(pattern_uuid, create->re_expression, ast_buffer.str(), true, time_stamp);
            // update cache
            global_binding_cache_manager->addReBinding(
                pattern_uuid, {create->re_expression, nullptr, create->settings()->clone(), re_ptr});
        }
    }
    else if (!create->query_pattern.empty() && create->target())
    {
        pattern_uuid
            = SQLBindingUtils::getQueryHash(create->query_pattern.data(), create->query_pattern.data() + create->query_pattern.size());
        if (create->level == BindingLevel::SESSION)
        {
            auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
            session_binding_cache_manager->addSqlBinding(
                pattern_uuid, {create->query_pattern, create->target()->clone(), nullptr, nullptr});
        }
        else
        {
            auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
            serializeAST(create->target(), ast_buffer);
            binding = std::make_shared<SQLBindingItem>(pattern_uuid, create->query_pattern, ast_buffer.str(), false);
            // update cache
            global_binding_cache_manager->addSqlBinding(pattern_uuid, {create->query_pattern, create->target()->clone(), nullptr, nullptr});
        }
    }
    else
        throw Exception("Create SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);

    // Update the cache of all servers
    if (create->level == BindingLevel::GLOBAL)
    {
        BindingCatalogManager catalog(context);
        catalog.updateSQLBinding(binding);
        for (int i = 0; i < 3; ++i)
        {
            try
            {
                catalog.updateGlobalBindingCache(context);
            }
            catch (...)
            {
                if (i == 2)
                    throw Exception(
                        "The global binding is successfully written to catalog and the current server cache, But can't sync to some other "
                        "server's cache",
                        ErrorCodes::LOGICAL_ERROR);
                continue;
            }
            break;
        }
    }

    BlockIO res;
    String output = "Create SQL Binding succeeded";
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
