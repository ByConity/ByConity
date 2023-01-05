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

        // sub-column storing all map keys of a Map column, e.g. mapKeys(map_col) ==> map_col.keys
        MAP_KEYS
    };

    Type type;
    String map_element_key;

    String getSubColumnName(const String &) const;

    bool operator==(const SubColumnID & other) const;

    struct Hash
    {
        size_t operator()(const SubColumnID & id) const;
    };

    static inline SubColumnID mapKeys()
    {
        return SubColumnID {Type::MAP_KEYS, ""};
    }

    static inline SubColumnID mapElement(const String & map_element_key)
    {
        return SubColumnID {Type::MAP_ELEMENT, map_element_key};
    }
};

}
