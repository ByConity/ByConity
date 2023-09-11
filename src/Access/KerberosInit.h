/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Copyright 2023 Bytedance Ltd. and/or its affiliates.
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

#pragma once

#include <Common/config.h>
#include <common/types.h>

#if USE_KRB5

// the default_kinit_timeout is 30min
constexpr time_t DEFAULT_KINIT_TIMEOUT = 30 * 60;

void kerberosInit(const String & keytab_file, const String & principal, const String & cache_name = "", time_t kinit_timeout = DEFAULT_KINIT_TIMEOUT);

#endif // USE_KRB5
