#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <atomic>

namespace DB
{

template <typename T>
struct AtomicSetting
{
    std::atomic<T> value;
    AtomicSetting(T t) : value(t) {}
    operator T() const { return get(); }
    AtomicSetting & operator=(T t) { set(t); return *this; }
    void set(T t) { value = t; }
    T get() const { return value.load(std::memory_order_relaxed); }
};

using AtomicSettingUInt64 = AtomicSetting<UInt64>;
using AtomicSettingBool = AtomicSetting<bool>;

/** Settings for Catalog
  */
struct CatalogSettings
{

#define APPLY_FOR_CATALOG_SETTINGS(M) \
    M(AtomicSettingUInt64, max_commit_size_one_batch, 500)                                                                 \
    M(AtomicSettingUInt64, max_drop_size_one_batch, 1000)                                                               \
    M(AtomicSettingBool, write_undo_buffer_new_key, false)                                               \

#define DECLARE(TYPE, NAME, DEFAULT) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_CATALOG_SETTINGS(DECLARE)
#undef DECLARE

public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};
}
