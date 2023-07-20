#pragma once

#include <initializer_list>
#include <list>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>
#include <Parsers/formatAST.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const ErrorCode LOGICAL_ERROR;
}

// this append only
template <typename Key, typename Hash = std::hash<Key>>
class LinkedHashSet
{
public:
    LinkedHashSet() = default;
    template <typename KeyArg>
    void emplace(KeyArg && arg)
    {
        if (set.template emplace(arg).second)
        {
            ordered_storage.emplace_back(std::forward<KeyArg>(arg));
        }
    }

    template <typename KeyArg>
    void insert(KeyArg && arg)
    {
        emplace(std::forward<KeyArg>(arg));
    }

    LinkedHashSet(std::initializer_list<Key> && init_list)
    {
        for (auto & arg : init_list)
        {
            emplace(arg);
        }
    }

    template <typename Iter>
    LinkedHashSet(Iter beg, Iter end)
    {
        insert(beg, end);
    }

    template <typename Iter>
    void insert(Iter beg, Iter end)
    {
        for (auto iter = beg; iter != end; ++iter)
        {
            this->emplace(*iter);
        }
    }

    // TODO: use user-defined key to avoid it
    // non-const iterate is not safe since
    // user may modify the value

    auto begin() { return ordered_storage.begin(); }
    auto end() { return ordered_storage.end(); }

    auto begin() const { return ordered_storage.cbegin(); }
    auto end() const { return ordered_storage.cend(); }

    size_t size() const { return ordered_storage.size(); }

    size_t count(const Key & key) const { return set.count(key); }

    bool empty() const { return ordered_storage.empty(); }

    const auto & front() const { return ordered_storage.front(); }

    const auto & back() const { return ordered_storage.back(); }

    bool operator==(const LinkedHashSet & other) const { return ordered_storage == other.ordered_storage; }

    LinkedHashSet(const LinkedHashSet &) = default;
    LinkedHashSet(LinkedHashSet &&) = default;

    LinkedHashSet & operator=(const LinkedHashSet &) = default;
    LinkedHashSet & operator=(LinkedHashSet &&) = default;

private:
    std::vector<Key> ordered_storage;
    std::unordered_set<Key, Hash> set;
};

} // namespace DB
