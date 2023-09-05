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

#include "registerTableFunctions.h"
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionNull(factory);
    registerTableFunctionZeros(factory);
    registerTableFunctionFile(factory);
    registerTableFunctionURL(factory);
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);
    registerTableFunctionGenerate(factory);

#if USE_AWS_S3
    registerTableFunctionS3(factory);
    registerTableFunctionCOS(factory);
#endif

#if USE_HDFS
    registerTableFunctionHDFS(factory);
#endif

    registerTableFunctionODBC(factory);
    registerTableFunctionJDBC(factory);

    registerTableFunctionView(factory);

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif

#if USE_LIBPQXX
    registerTableFunctionPostgreSQL(factory);
#endif

#if USE_HIVE
    registerTableFunctionCnchHive(factory);
    // registerTableFunctionCloudHive(factory);
    registerTableFunctionHiveMetadata(factory);
#endif

    registerTableFunctionDictionary(factory);
    registerTableFunctionCnch(factory);
}

}
