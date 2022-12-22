#pragma once

#include <QueryPlan/PlanNode.h>

#include <any>
#include <atomic>
#include <unordered_map>
#include <utility>

namespace DB
{

class Capture
{
public:
    explicit Capture(const std::string& type = "") : desc(type + "@" + std::to_string(id++)){}

    bool operator==(const Capture & other) const
    {
        return desc == other.desc;
    }

    struct hash
    {
        std::size_t operator()(const Capture& c) const
        {
            return std::hash<std::string>()(c.desc);
        }
    };

    std::string desc;

private:
    static std::atomic_uint64_t id;
};

class Captures: public std::unordered_multimap<Capture, std::any, Capture::hash>
{
public:
    template <typename T>
    T at(const Capture & capture) const
    {
        auto iters = equal_range(capture);
        auto next = iters.first;

        if (iters.first == iters.second || ++next != iters.second) {
            throw Exception("Not unique capture for this capture key: " + capture.desc, ErrorCodes::LOGICAL_ERROR);
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
