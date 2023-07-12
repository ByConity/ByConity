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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Storages/registerStorages.h>
#include <QueryPlan/Hints/registerHints.h>

inline void tryRegisterFunctions()
{
    static struct Register { Register() { DB::registerFunctions(); } } registered;
}

inline void tryRegisterFormats()
{
    static struct Register { Register() { DB::registerFormats(); } } registered;
}

inline void tryRegisterStorages()
{
    static struct Register { Register() { DB::registerStorages(); } } registered;
}

inline void tryRegisterAggregateFunctions()
{
    static struct Register { Register() { DB::registerAggregateFunctions(); } } registered;
}

inline void tryRegisterHints()
{
    static struct Register { Register() { DB::registerHints(); } } registered;
}
