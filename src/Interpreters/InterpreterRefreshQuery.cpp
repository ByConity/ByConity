#include <Interpreters/InterpreterRefreshQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/InterpreterSetQuery.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }

    BlockIO InterpreterRefreshQuery::execute()
    {
        const auto & refresh = query_ptr->as<ASTRefreshQuery &>();
        if (refresh.settings_ast)
            InterpreterSetQuery(refresh.settings_ast, getContext()).executeForCurrentContext();

        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(DatabaseCatalog::instance().getTable({refresh.database, refresh.table}, getContext()).get());
        if (!materialized_view)
        {
            String db_str = refresh.database.empty() ? "" : backQuoteIfNeed(refresh.database) + ".";
            throw Exception("Table " + db_str + backQuoteIfNeed(refresh.table) +
                            " isn't a materialized view, can't be refreshed.", ErrorCodes::LOGICAL_ERROR);
        }

        materialized_view->refresh(refresh.partition, getContext(), refresh.async);

        return {};
    }
}
