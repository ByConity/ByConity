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

#include <Core/Field.h>


namespace DB
{
namespace Protos
{
    class SettingChange;
    class SettingsChanges;
}
struct SettingChange
{
    String name;
    Field value;

    SettingChange() {}
    SettingChange(const std::string_view & name_, const Field & value_) : name(name_), value(value_) {}
    SettingChange(const std::string_view & name_, Field && value_) : name(name_), value(std::move(value_)) {}

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value); }
    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
    void toProto(Protos::SettingChange & proto) const;
    void fillFromProto(const Protos::SettingChange & proto);
};


class SettingsChanges : public std::vector<SettingChange>
{
public:
    using std::vector<SettingChange>::vector;

    bool tryGet(const std::string_view & name, Field & out_value) const;
    const Field * tryGet(const std::string_view & name) const;
    Field * tryGet(const std::string_view & name);

    /// Inserts element if doesn't exists and returns true, otherwise just returns false
    bool insertSetting(std::string_view name, const Field & value);
    /// Sets element to value, inserts if doesn't exist
    void setSetting(std::string_view name, const Field & value);
    /// If element exists - removes it and returns true, otherwise returns false
    bool removeSetting(std::string_view name);

    void merge(const SettingsChanges & other);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
    void toProto(Protos::SettingsChanges & proto) const;
    void fillFromProto(const Protos::SettingsChanges & proto);

    static std::unordered_set<String> WHITELIST_SETTINGS;
};

}
