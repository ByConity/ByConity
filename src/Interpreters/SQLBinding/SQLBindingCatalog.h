#pragma once
#include <memory>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/SQLBinding/SQLBinding.h>

namespace DB
{
class BindingCatalogManager
{
public:
    explicit BindingCatalogManager(const ContextPtr & context)
    {
        catalog = context->getCnchCatalog();
    }

    void updateSQLBinding(const SQLBindingItemPtr & data);

    SQLBindings getSQLBindings();

    SQLBindings getReSQLBindings();

    SQLBindingItemPtr getSQLBinding(const UUID & uuid, const String & tenant_id, const bool & is_re_expression);

    void removeSQLBinding(const UUID & uuid, const String & tenant_id, const bool & is_re_expression);

    static void updateGlobalBindingCache(const ContextPtr & context);

private:
    std::shared_ptr<Catalog::Catalog> catalog;
};

}

