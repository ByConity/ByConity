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

#include <Storages/IStorage.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Access/AccessFlags.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserDataType.h>


namespace DB
{

BlockIO InterpreterDescribeQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterDescribeQuery::getSampleBlock(ContextPtr context_, bool include_subcolumns)
{
    Block block;

    ColumnWithTypeAndName col;
    col.name = "name";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "type";
    block.insert(col);

    if (context_->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
    {
        col.name = "nullable";
        block.insert(col);
    }

    col.name = "flags";
    block.insert(col);

    col.name = "default_type";
    block.insert(col);

    col.name = "default_expression";
    block.insert(col);

    col.name = "comment";
    block.insert(col);

    col.name = "codec_expression";
    block.insert(col);

    col.name = "ttl_expression";
    block.insert(col);

    if (include_subcolumns)
    {
        col.name = "is_subcolumn";
        col.type = std::make_shared<DataTypeUInt8>();
        col.column = col.type->createColumn();
        block.insert(col);
    }

    return block;
}


BlockInputStreamPtr InterpreterDescribeQuery::executeImpl()
{
    ColumnsDescription columns;
    StorageSnapshotPtr storage_snapshot;

    const auto & ast = query_ptr->as<ASTDescribeQuery &>();
    const auto & table_expression = ast.table_expression->as<ASTTableExpression &>();
    const auto & settings = getContext()->getSettingsRef();

    if (table_expression.subquery)
    {
        auto names_and_types = InterpreterSelectWithUnionQuery::getSampleBlock(
            table_expression.subquery->children.at(0), getContext()).getNamesAndTypesList();
        columns = ColumnsDescription(std::move(names_and_types));
    }
    else if (table_expression.table_function)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression.table_function, getContext());
        columns = table_function_ptr->getActualTableStructure(getContext());
    }
    else
    {
        auto table_id = getContext()->resolveStorageID(table_expression.database_and_table_name);
        getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);
        auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
        auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings.lock_acquire_timeout);
        auto metadata_snapshot = table->getInMemoryMetadataPtr();
        storage_snapshot = table->getStorageSnapshot(metadata_snapshot, getContext());
        columns = metadata_snapshot->getColumns();
    }

    bool extend_object_types = settings.describe_extend_object_types && storage_snapshot;
    bool include_subcolumns = settings.describe_include_subcolumns;
    Block sample_block = getSampleBlock(getContext(), include_subcolumns);
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    auto dialect_type = getContext()->getSettingsRef().dialect_type;

    for (const auto & column : columns)
    {
        size_t i = 0;
        res_columns[i++]->insert(column.name);
        if (extend_object_types)
            res_columns[i++]->insert(storage_snapshot->getConcreteType(column.name)->getName());
        else if (dialect_type != DialectType::CLICKHOUSE)
        {
            /// Under ANSI mode, data type will be parsed by ANSI type parser, which converts Nullable to
            /// null modifiers. And the nullability of root type will be demonstrated in the separate
            /// field: `nullable`. For instance, the type field Nullable(Array(Array(Nullable(Array(String)))))
            /// will be divided into two fields under ANSI mode:
            ///     type: Array(Array(Array(String NOT NULL) NULL) NOT NULL
            ///     nullable: true
            ParserDataType type_parser(ParserSettings::ANSI);
            String type_name = column.type->getName();
            const char * type_name_pos = type_name.data();
            const char * type_name_end = type_name_pos + type_name.size();
            auto type_ast = parseQuery(type_parser, type_name_pos, type_name_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

            bool nullable;
            if (const auto * t_ast = type_ast->as<ASTDataType>())
            {
                nullable = t_ast->getNullable();
                type_ast = t_ast->getNestedType();
            }
            else
                nullable = false;

            WriteBufferFromOwnString buf;
            formatAST(*type_ast, buf, false, true, false, dialect_type);
            res_columns[i++]->insert(buf.str());
            res_columns[i++]->insert(nullable ? "true" : "false");
        }
        else
            res_columns[i++]->insert(column.type->getName());

        /// Data type flags if contains map
        {
            String flags_str;
            if (column.type->isMap() || column.type->hasNestedMap())
            {
                /// Map can only be Byte Map or KV Map, all nested map is KV Map
                if (column.type->isByteMap())
                    flags_str = " BYTE";
                else
                    flags_str = " KV";
            }
            res_columns[i++]->insert(flags_str);
        }

        if (column.default_desc.expression)
        {
            res_columns[i++]->insert(toString(column.default_desc.kind));
            res_columns[i++]->insert(queryToString(column.default_desc.expression));
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(column.comment);

        if (column.codec)
            res_columns[i++]->insert(queryToString(column.codec->as<ASTFunction>()->arguments));
        else
            res_columns[i++]->insertDefault();

        if (column.ttl)
            res_columns[i++]->insert(queryToString(column.ttl));
        else
            res_columns[i++]->insertDefault();
    }

    if (include_subcolumns)
    {
        for (const auto & column : columns)
        {
            auto type = extend_object_types ? storage_snapshot->getConcreteType(column.name) : column.type;

            IDataType::forEachSubcolumn([&](const auto & path, const auto & name, const auto & data)
            {
                res_columns[0]->insert(Nested::concatenateName(column.name, name));
                res_columns[1]->insert(data.type->getName());

                /// It's not trivial to calculate default expression for subcolumn.
                /// So, leave it empty.
                res_columns[2]->insertDefault();
                res_columns[3]->insertDefault();
                res_columns[4]->insert(column.comment);

                if (column.codec && ISerialization::isSpecialCompressionAllowed(path))
                    res_columns[5]->insert(queryToString(column.codec->as<ASTFunction>()->arguments));
                else
                    res_columns[5]->insertDefault();

                if (column.ttl)
                    res_columns[6]->insert(queryToString(column.ttl));
                else
                    res_columns[6]->insertDefault();

                res_columns[7]->insert(1u);
            }, ISerialization::SubstreamData(type->getDefaultSerialization()).withType(type));
        }
    }

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}
