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

#pragma once

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/SegmentScheduler.h>
#include <Processors/QueryPipeline.h>

#include <Protos/cnch_common.pb.h>

using AsyncQueryStatus = DB::Protos::AsyncQueryStatus;

namespace DB
{
class ReadBuffer;
class WriteBuffer;
class MPPQueryCoordinator;
using MPPQueryCoordinatorPtr = std::shared_ptr<MPPQueryCoordinator>;


/// Parse and execute a query.
void executeQuery(
    ReadBuffer & istr,                  /// Where to read query from (and data for INSERT, if present).
    WriteBuffer & ostr,                 /// Where to write query output to.
    bool allow_into_outfile,            /// If true and the query contains INTO OUTFILE section, redirect output to that file.
    ContextMutablePtr context,                 /// DB, tables, data types, storage engines, functions, aggregate functions...
    std::function<void(const String &, const String &, const String &, const String &, MPPQueryCoordinatorPtr)> set_result_details, /// If a non-empty callback is passed, it will be called with the query id, the content-type, the format, and the timezone.
    const std::optional<FormatSettings> & output_format_settings = std::nullopt, /// Format settings for output format, will be calculated from the context if not set.
    bool internal = false /// If a query is a internal which cannot be inserted into processlist.
);


/// More low-level function for server-to-server interaction.
/// Prepares a query for execution but doesn't execute it.
/// Returns a pair of block streams which, when used, will result in query execution.
/// This means that the caller can to the extent control the query execution pipeline.
///
/// To execute:
/// * if present, write INSERT data into BlockIO::out
/// * then read the results from BlockIO::in.
///
/// If the query doesn't involve data insertion or returning of results, out and in respectively
/// will be equal to nullptr.
///
/// Correctly formatting the results (according to INTO OUTFILE and FORMAT sections)
/// must be done separately.
BlockIO executeQuery(
    const String & query, /// Query text without INSERT data. The latter must be written to BlockIO::out.
    ContextMutablePtr context, /// DB, tables, data types, storage engines, functions, aggregate functions...
    bool internal = false, /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete, /// To which stage the query must be executed.
    bool may_have_embedded_data = false /// If insert query may have embedded data
);

BlockIO executeQuery(
    const String & query,
    ASTPtr ast,
    ContextMutablePtr context,
    bool internal = false,    /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete,    /// To which stage the query must be executed.
    bool may_have_embedded_data  = false /// If insert query may have embedded data
);

/// Old interface with allow_processors flag. For compatibility.
BlockIO executeQuery(
    const String & query,
    ContextMutablePtr context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data,
    bool allow_processors /// If can use processors pipeline
);

/// For interactive transaction

/// Return true if current query is in an interactive transaction session
bool isQueryInInteractiveSession(const ContextPtr & context, [[maybe_unused]] const ASTPtr & query = nullptr);

// Return true if query is ddl
bool isDDLQuery(const ContextPtr & context, const ASTPtr & query);

/// Async query execution

bool isAsyncMode(ContextMutablePtr context);

void executeHttpQueryInAsyncMode(
    String & query,
    ASTPtr ast,
    ContextMutablePtr c,
    WriteBuffer & ostr,
    ReadBuffer * istr,
    bool has_query_tail,
    const std::optional<FormatSettings> & f,
    std::function<void(const String &, const String &, const String &, const String &, MPPQueryCoordinatorPtr)> set_result_details);

void updateAsyncQueryStatus(
    ContextMutablePtr context,
    const String & async_query_id,
    const String & query_id,
    const AsyncQueryStatus::Status & status,
    const String & error_msg = "");

void interpretSettings(ASTPtr ast, ContextMutablePtr context);

bool needThrowRootCauseError(const Context * context, int & error_code, String & error_messge);

}
