#pragma once

#include <array>
#include <optional>
#include <set>
#include <Parsers/ASTDataType.h>
#include <Storages/IStorage.h>
#include <Poco/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FEATURE_NOT_SUPPORT_GIS;
    extern const int LOGICAL_ERROR;
}


/**
 * @class AdditionalService
 * @brief Represent a service.
 */
class AdditionalService
{
public:
    /// Currently using bitmap. But it's valnerable to reverse engineering.
    enum Value
    {
        GIS = 0,
        VectorSearch = 1,
        FullTextSearch = 2,
        SIZE = 3,
    };
    static constexpr std::array<const char *, Value::SIZE> ValueS = {"GIS", "VectorSearch", "FullTextSearch"};

    explicit AdditionalService(std::string svc_s) noexcept;
    explicit AdditionalService(AdditionalService::Value svc) : additional_service(svc) { }

    /**
     * @brief Deserialize string to enum value.
     *
     * @param svc_s One of `ValueS`.
     * @return Return enum `SIZE` if input is invalid.
     */
    static AdditionalService tryDeserialize(std::string svc_s) noexcept;

    std::string toString() { return ValueS[additional_service]; }
    explicit operator int() const { return additional_service; }
    explicit operator Value() const { return additional_service; }
    Value value() const { return additional_service; }

private:
    Value additional_service;
};

/**
 * @class AdditionalServices
 * @brief A thread safe class that store whether an additional feature is enabled.
 */
class AdditionalServices
{
    std::atomic<size_t> enabled_services{};

    /// Fully replaced with new value.
    void repace(size_t new_enabled_services);

public:
    AdditionalServices() = default;

    /// Do not allow copy.
    AdditionalServices(const AdditionalService &) = delete;
    AdditionalServices operator=(const AdditionalServices &) = delete;

    /// Update state according to the `config`.
    void parseAdditionalServicesFromConfig(const Poco::Util::AbstractConfiguration & config);

    size_t size() const;

    /// Helper functions for judging whether a service is enabled.
    bool enabled(AdditionalService::Value svc) const;
    void throwIfDisabled(AdditionalService::Value svc) const;
};

}
