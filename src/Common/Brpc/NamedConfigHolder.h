#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <type_traits>
#include <Common/Brpc/BaseConfigHolder.h>

namespace DB
{
/// NamedConfigHolder has a unique name to match certain config path pattern,
/// which is defined in derived class as static inline variable.
template <typename TDerived, typename TConfig, typename TDelete>
class NamedConfigHolder : public BaseConfigHolder
{
    using ConfigReloadCallback = std::function<void(const TConfig * old_conf_ptr, const TConfig * new_conf_ptr)>;

public:
    explicit NamedConfigHolder()
    {
        static_assert(
            std::is_same<std::string, decltype(TDerived::name)>::value, "A static inline std::string TDerived::name should exist!");
    }

    /// Create config entity (custom struct) from raw config
    virtual std::unique_ptr<TConfig, TDelete> createTypedConfig(RawConfAutoPtr) noexcept { return nullptr; }

    /// Callback for ConfigHolder on configuration first load
    virtual void afterInit(const TConfig * config_ptr) = 0;

    /// Compare configuration changes, return true if it is changed, mind null old_conf_ptr is acceptable
    virtual bool hasChanged(const TConfig * old_conf_ptr, const TConfig * new_conf_ptr) = 0;

    /// Callback for ConfigHolder when configuration changed, mind null old_conf_ptr is acceptable
    virtual void onChange(const TConfig * old_conf_ptr, const TConfig * new_conf_ptr) = 0;

    /// Callback for Custom listener on configuration changed
    void initReloadCallback(ConfigReloadCallback callback);

protected:
    ConfigReloadCallback reload_callback;
};

template <typename TDerived, typename TConfig, typename TDelete>
void NamedConfigHolder<TDerived, TConfig, TDelete>::initReloadCallback(ConfigReloadCallback callback)
{
    static std::once_flag init_flag;
    std::call_once(init_flag, [this, &callback] { this->reload_callback = std::move(callback); });
}

}
