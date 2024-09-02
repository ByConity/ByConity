#pragma once
#include <memory>
#include <Core/Types.h>
#include <Interpreters/Context.h>

namespace DB
{

class PreparedStatementItem
{

public:
    PreparedStatementItem(String name_, String create_statement_)
        : name(std::move(name_))
        , create_statement(std::move(create_statement_))
    {
    }

    String name;
    String create_statement;
};

using PreparedStatementItemPtr = std::shared_ptr<PreparedStatementItem>;
using PreparedStatements = std::vector<PreparedStatementItemPtr>;

class PreparedStatementCatalogManager
{
public:
    explicit PreparedStatementCatalogManager(const ContextPtr & context)
    {
        catalog = context->getCnchCatalog();
    }

    void updatePreparedStatement(PreparedStatementItemPtr);

    PreparedStatements getPreparedStatements();

    PreparedStatementItemPtr getPreparedStatement(const String & name);

    void removePreparedStatement(const String & name);

private:
    std::shared_ptr<Catalog::Catalog> catalog;
};

}
