/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <QueryPlan/PlanNode.h>

#include <any>
#include <atomic>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace DB
{

using Capture = std::string_view;

class Captures : public std::unordered_multimap<Capture, std::any>
{
public:
    template <typename T>
    T at(const Capture & capture) const
    {
        auto iters = equal_range(capture);
        auto next = iters.first;

        if (iters.first == iters.second || ++next != iters.second) {
            throw Exception("Not unique capture for this capture key: " + String{capture}, ErrorCodes::LOGICAL_ERROR);
        }

        return std::any_cast<T>(iters.first->second);
    }
};

class Match
{
public:
    explicit Match(const Captures & captures_): captures(captures_) {} // NOLINT
    explicit Match(Captures && captures_): captures(std::move(captures_)) {}

    Captures captures;
};

}
