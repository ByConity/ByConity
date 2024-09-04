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

#include <Core/Types.h>

namespace DB
{

struct SubColumnID
{
    enum class Type
    {
        // sub-columns for each map element of a Map column, e.g. map_col{'a'} ==> __map_col__a
        MAP_ELEMENT,

        // sub-column storing all map keys of a Map column, e.g. mapKeys(map_col) ==> map_col.key
        MAP_KEYS,

        // sub-column storing all map values of a Map column, e.g. mapValues(map_col) ==> map_col.value
        MAP_VALUES,

        JSON_FIELD,
    };

    Type type;
    String map_element_key;
    String json_field_name;

    String getSubColumnName(const String &) const;

    bool operator==(const SubColumnID & other) const;

    struct Hash
    {
        size_t operator()(const SubColumnID & id) const;
    };

    static inline SubColumnID mapElement(const String & map_element_key)
    {
        SubColumnID id;
        id.type = Type::MAP_ELEMENT;
        id.map_element_key = map_element_key;
        return id;
    }

    static inline SubColumnID mapKeys()
    {
        SubColumnID id;
        id.type = Type::MAP_KEYS;
        return id;
    }

    static inline SubColumnID mapValues()
    {
        SubColumnID id;
        id.type = Type::MAP_VALUES;
        return id;
    }

    static inline SubColumnID jsonField(const String & json_field_name)
    {
        SubColumnID id;
        id.type = Type::JSON_FIELD;
        id.json_field_name = json_field_name;
        return id;
    }
};

}
