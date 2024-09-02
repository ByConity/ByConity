/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTBitEngineConstraintDeclaration.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTStorage::clone() const
{
    auto res = std::make_shared<ASTStorage>(*this);
    res->children.clear();

    if (engine)
        res->set(res->engine, engine->clone());
    if (partition_by)
        res->set(res->partition_by, partition_by->clone());
    if (cluster_by)
        res->set(res->cluster_by, cluster_by->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());
    if (order_by)
        res->set(res->order_by, order_by->clone());
    if (unique_key)
        res->set(res->unique_key, unique_key->clone());
    if (sample_by)
        res->set(res->sample_by, sample_by->clone());
    if (ttl_table)
        res->set(res->ttl_table, ttl_table->clone());

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTStorage::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (engine)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
        engine->formatImpl(s, state, frame);
    }
    if (partition_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY " << (s.hilite ? hilite_none : "");
        partition_by->formatImpl(s, state, frame);
    }
    if (cluster_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "CLUSTER BY " << (s.hilite ? hilite_none : "");
        cluster_by->formatImpl(s, state, frame);
    }
    if (primary_key)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PRIMARY KEY " << (s.hilite ? hilite_none : "");
        primary_key->formatImpl(s, state, frame);
    }
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


class ASTColumnsElement : public IAST
{
public:
    String prefix;
    IAST * elem;

    String getID(char c) const override { return "ASTColumnsElement for " + elem->getID(c); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

ASTPtr ASTColumnsElement::clone() const
{
    auto res = std::make_shared<ASTColumnsElement>();
    res->prefix = prefix;
    if (elem)
        res->set(res->elem, elem->clone());
    return res;
}

void ASTColumnsElement::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (!elem)
        return;

    if (prefix.empty())
    {
        elem->formatImpl(s, state, frame);
        return;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << prefix << (s.hilite ? hilite_none : "");
    s.ostr << ' ';
    elem->formatImpl(s, state, frame);
}


ASTPtr ASTColumns::clone() const
{
    auto res = std::make_shared<ASTColumns>();

    if (columns)
        res->set(res->columns, columns->clone());
    if (indices)
        res->set(res->indices, indices->clone());
    if (constraints)
        res->set(res->constraints, constraints->clone());
    if (foreign_keys)
        res->set(res->foreign_keys, foreign_keys->clone());
    if (unique)
        res->set(res->unique, unique->clone());
    if (projections)
        res->set(res->projections, projections->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());
    if (mysql_indices)
        res->set(res->mysql_indices, mysql_indices->clone());

    return res;
}

void ASTColumns::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ASTExpressionList list;

    if (columns)
    {
        for (const auto & column : columns->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "";
            elem->set(elem->elem, column->clone());
            list.children.push_back(elem);
        }
    }
    if (indices)
    {
        for (const auto & index : indices->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "INDEX";
            elem->set(elem->elem, index->clone());
            list.children.push_back(elem);
        }
    }
    if (constraints)
    {
        for (const auto & constraint : constraints->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            if (constraint->as<ASTBitEngineConstraintDeclaration>())
                elem->prefix = "BITENGINE_CONSTRAINT";
            else
                elem->prefix = "CONSTRAINT";
            elem->set(elem->elem, constraint->clone());
            list.children.push_back(elem);
        }
    }
    if (projections)
    {
        for (const auto & projection : projections->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "PROJECTION";
            elem->set(elem->elem, projection->clone());
            list.children.push_back(elem);
        }
    }
    if (foreign_keys)
    {
        for (const auto & foreign_key : foreign_keys->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "CONSTRAINT";
            elem->set(elem->elem, foreign_key->clone());
            list.children.push_back(elem);
        }
    }
    if (unique)
    {
        for (const auto & unique_key : unique->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "CONSTRAINT";
            elem->set(elem->elem, unique_key->clone());
            list.children.push_back(elem);
        }
    }
    if (mysql_indices)
    {
        for (const auto & mysql_index : mysql_indices->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->set(elem->elem, mysql_index->clone());
            list.children.push_back(elem);
        }
    }

    if (!list.children.empty())
    {
        if (s.one_line)
            list.formatImpl(s, state, frame);
        else
            list.formatImplMultiline(s, state, frame);
    }
}

ASTPtr ASTCreateSnapshotQuery::clone() const
{
    auto res = std::make_shared<ASTCreateSnapshotQuery>(*this);
    cloneOutputOptions(*res);
    return res;
}

void ASTCreateSnapshotQuery::formatQueryImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE SNAPSHOT " << (if_not_exists ? "IF NOT EXISTS " : "")
                  << (settings.hilite ? hilite_none : "");

    settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
    if (uuid != UUIDHelpers::Nil)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                      << quoteString(toString(uuid));

    if (to_table_id)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
                      << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
                      << backQuoteIfNeed(to_table_id.table_name);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " TTL " << (settings.hilite ? hilite_none : "") << ttl_in_days
                  << (settings.hilite ? hilite_keyword : "") << " DAYS " << (settings.hilite ? hilite_none : "");
}

ASTPtr ASTCreateQuery::clone() const
{
    auto res = std::make_shared<ASTCreateQuery>(*this);
    res->children.clear();

    if (columns_list && !columns_list->empty())
        res->set(res->columns_list, columns_list->clone());
    if (storage)
        res->set(res->storage, storage->clone());
    if (select)
        res->set(res->select, select->clone());
    if (refresh_strategy)
        res->set(res->refresh_strategy, refresh_strategy->clone());
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

void ASTCreateQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (!catalog.empty() && database.empty() && table.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE EXTERNAL CATALOG " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(catalog) << " PROPERTIES ";
        if (catalog_properties)
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
        /// replace "\n(" to "(\n" for MYSQL as navicat (duplicate table action) requires
        settings.ostr << ((settings.one_line || settings.dialect_type == DialectType::MYSQL) ? " (" : "\n(");
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

    if (dictionary)
        dictionary->formatImpl(settings, state, frame);

    if (is_populate)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");

    if (refresh_strategy)
    {
        settings.ostr << settings.nl_or_ws;
        refresh_strategy->formatImpl(settings, state, frame);
    }

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

void ASTCreateQuery::toLowerCase()
{
    boost::to_lower(database);
    boost::to_lower(table);
    boost::to_lower(as_database);
    boost::to_lower(as_table);
}

void ASTCreateQuery::toUpperCase()
{
    boost::to_upper(database);
    boost::to_upper(table);
    boost::to_upper(as_database);
    boost::to_upper(as_table);
}

}
