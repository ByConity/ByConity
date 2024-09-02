#include <memory>
#include <Parsers/ASTCreateQueryAnalyticalMySQL.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTBitEngineConstraintDeclaration.h>
#include <Common/quoteString.h>
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/IAST_fwd.h"
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>


namespace DB
{
void ASTStorageAnalyticalMySQL::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (mysql_engine)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
        mysql_engine->formatImpl(s, state, frame);
    }
    else if (engine)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
        engine->formatImpl(s, state, frame);
    }

    if (mysql_partition_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY VALUE(" << (s.hilite ? hilite_none : "");
        mysql_partition_by->formatImpl(s, state, frame);
        s.ostr << ")";

        if (life_cycle)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "LIFECYCLE " << (s.hilite ? hilite_none : "");
            life_cycle->formatImpl(s, state, frame);
        }
    }
    else if (partition_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY " << (s.hilite ? hilite_none : "");
        partition_by->formatImpl(s, state, frame);
    }

    if (distributed_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "DISTRIBUTED BY HASH(" << (s.hilite ? hilite_none : "");
        distributed_by->formatImpl(s, state, frame);
        s.ostr << ")";
    }
    else if (cluster_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "CLUSTER BY " << (s.hilite ? hilite_none : "");
        cluster_by->formatImpl(s, state, frame);
    }

    if (mysql_primary_key)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PRIMARY KEY(" << (s.hilite ? hilite_none : "");
        primary_key->formatImpl(s, state, frame);
        s.ostr << ")";
    }
    else if (primary_key)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PRIMARY KEY " << (s.hilite ? hilite_none : "");
        primary_key->formatImpl(s, state, frame);
    }

    // MySQL only
    if (storage_policy)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "STORAGE_POLICY = " << (s.hilite ? hilite_none : "");
        storage_policy->formatImpl(s, state, frame);

        if (hot_partition_count)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "hot_partition_count = " << (s.hilite ? hilite_none : "");
            hot_partition_count->formatImpl(s, state, frame);
        }
    }

    if (block_size)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "BLOCK_SIZE = " << (s.hilite ? hilite_none : "");
        block_size->formatImpl(s, state, frame);
    }
    if (rt_engine)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "RT_ENGINE = " << (s.hilite ? hilite_none : "");
        rt_engine->formatImpl(s, state, frame);
    }
    if (table_properties)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "TABLE_PROPERTIES = " << (s.hilite ? hilite_none : "");
        table_properties->formatImpl(s, state, frame);
    }

    // Clickhouse
    if (order_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ORDER BY " << (s.hilite ? hilite_none : "");
        order_by->formatImpl(s, state, frame);
    }
    if (unique_key)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "UNIQUE KEY " << (s.hilite ? hilite_none : "");
        unique_key->formatImpl(s, state, frame);
    }
    if (sample_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SAMPLE BY " << (s.hilite ? hilite_none : "");
        sample_by->formatImpl(s, state, frame);
    }
    if (ttl_table)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "TTL " << (s.hilite ? hilite_none : "");
        ttl_table->formatImpl(s, state, frame);
    }
    if (settings && !settings->changes.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }

}

ASTPtr ASTCreateQueryAnalyticalMySQL::transform() const
{
    // this function is designed for transform mysql ast to ck ast
    // only ck class members will be kept
    return ASTCreateQuery::clone();
}

ASTPtr ASTCreateQueryAnalyticalMySQL::clone() const
{
    auto res = std::make_shared<ASTCreateQueryAnalyticalMySQL>(*this);
    res->children.clear();

    if (columns_list && !columns_list->empty())
        res->set(res->columns_list, columns_list->clone());
    if (storage)
        res->set(res->storage, storage->clone());
    if (select)
        res->set(res->select, select->clone());
    if (tables)
        res->set(res->tables, tables->clone());
    if (table_overrides)
        res->set(res->table_overrides, table_overrides->clone());

    if (dictionary)
    {
        assert(is_dictionary);
        res->set(res->dictionary_attributes_list, dictionary_attributes_list->clone());
        res->set(res->dictionary, dictionary->clone());
    }

    if (comment)
        res->set(res->comment, comment->clone());

    cloneOutputOptions(*res);

    return res;
}

void ASTCreateQueryAnalyticalMySQL::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (!catalog.empty() && database.empty() && table.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << "CREATE EXTERNAL CATALOG "
            << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << backQuoteIfNeed(catalog)
            << " PROPERTIES ";
        if(catalog_properties)
            catalog_properties->formatImpl(settings, state, frame);
        return;
    }

    if (!database.empty() && table.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
            << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << backQuoteIfNeed(database);

        if (uuid != UUIDHelpers::Nil)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        }

        formatOnCluster(settings);

        if (storage)
            storage->formatImpl(settings, state, frame);

        if (table_overrides)
        {
            settings.ostr << settings.nl_or_ws;
            table_overrides->formatImpl(settings, state, frame);
        }

        if (comment)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
            comment->formatImpl(settings, state, frame);
        }

        return;
    }

    if (!is_dictionary)
    {
        String action = "CREATE";
        if (attach)
            action = "ATTACH";
        else if (replace_view)
            action = "CREATE OR REPLACE";
        else if (replace_table && create_or_replace)
            action = "CREATE OR REPLACE";
        else if (replace_table)
            action = "REPLACE";

        String what = "TABLE";
        if (is_ordinary_view)
            what = "VIEW";
        else if (is_materialized_view)
            what = "MATERIALIZED VIEW";
        else if (is_live_view)
            what = "LIVE VIEW";

        settings.ostr
            << (settings.hilite ? hilite_keyword : "")
                << action << " "
                << (temporary ? "TEMPORARY " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));

        assert(attach || !attach_from_path);
        if (attach_from_path)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                          << quoteString(*attach_from_path);

        if (live_view_timeout)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH TIMEOUT " << (settings.hilite ? hilite_none : "")
                          << *live_view_timeout;

        if (live_view_periodic_refresh)
        {
            if (live_view_timeout)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " AND" << (settings.hilite ? hilite_none : "");
            else
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH" << (settings.hilite ? hilite_none : "");

            settings.ostr << (settings.hilite ? hilite_keyword : "") << " PERIODIC REFRESH " << (settings.hilite ? hilite_none : "")
                << *live_view_periodic_refresh;
        }

        formatOnCluster(settings);
    }
    else
    {
        String action = "CREATE";
        if (attach)
            action = "ATTACH";
        else if (replace_table && create_or_replace)
            action = "CREATE OR REPLACE";
        else if (replace_table)
            action = "REPLACE";

        /// Always DICTIONARY
        settings.ostr << (settings.hilite ? hilite_keyword : "") << action << " DICTIONARY "
                      << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "")
                      << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        formatOnCluster(settings);
    }

    if (to_table_id)
    {
        assert(is_materialized_view && to_inner_uuid == UUIDHelpers::Nil);
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
            << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
            << backQuoteIfNeed(to_table_id.table_name);
    }

    if (to_inner_uuid != UUIDHelpers::Nil)
    {
        assert(is_materialized_view && !to_table_id);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO INNER UUID " << (settings.hilite ? hilite_none : "")
                      << quoteString(toString(to_inner_uuid));
    }

    if (!as_table.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (as_table_function)
    {
        if (columns_list)
        {
            frame.expression_list_always_start_on_new_line = true;
            settings.ostr << (settings.one_line ? " (" : "\n(");
            FormatStateStacked frame_nested = frame;
            columns_list->formatImpl(settings, state, frame_nested);
            settings.ostr << (settings.one_line ? ")" : "\n)");
            frame.expression_list_always_start_on_new_line = false; //-V519
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        as_table_function->formatImpl(settings, state, frame);
    }

    frame.expression_list_always_start_on_new_line = true;

    if (columns_list && !columns_list->empty() && !as_table_function)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        columns_list->formatImpl(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    if (dictionary_attributes_list)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        if (settings.one_line)
            dictionary_attributes_list->formatImpl(settings, state, frame_nested);
        else
            dictionary_attributes_list->formatImplMultiline(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    frame.expression_list_always_start_on_new_line = false; //-V519

    if (storage)
        storage->formatImpl(settings, state, frame);

    if (mysql_storage)
        mysql_storage->formatImpl(settings, state, frame);

    if (dictionary)
        dictionary->formatImpl(settings, state, frame);

    if (is_populate)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");

    if (select)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << settings.nl_or_ws << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
    }

    if (tables)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH " << (settings.hilite ? hilite_none : "");
        tables->formatImpl(settings, state, frame);
    }

    if (comment)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
        comment->formatImpl(settings, state, frame);
    }
}

}
