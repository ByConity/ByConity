#pragma once

#include <unordered_map>
#include <vector>
#include <atomic>
#include <chrono>
#include <functional>
#include <list>
#include <shared_mutex>
#include <memory>
#include <common/defines.h>

namespace DB
{

/// @brief  Wait free map for scan operation
/// @tparam Key unique
/// @tparam Value needs contain key info
/**
 * Based on HashMap, List and Atomic pointer
 * Scan operation is wait-free
 * Find and insert operator still need acquire lock
 * Removed items will be released immediatly if no iterators or released later with a lifetime protection
 * The GC operator happens mainly in write operations, but also could happen in scan operations
*/
template<typename Key, typename Value>
class ScanWaitFreeMap
{
public:
    using KeysVec = std::vector<Key>;
    using ValuesVec = std::vector<Value>;
    using GetKeyFunc=std::function<Key(const Value&)>;
    using Delay = std::chrono::seconds;

    struct Node
    {
        Value value;
        std::atomic<Node*> next;
        Node* prev;
    };

    struct Iterator
    {
        using iterator_category = std::forward_iterator_tag;
        using difference_type   = std::ptrdiff_t;
        using value_type = Value;
        using pointer = Value*;
        using reference = Value&;
    
        Iterator(Iterator&& other) : m_ptr(other.m_ptr), map(other.map)
        {
            /// Avoid unneccesary descrease counter in other's destructor
            other.map = nullptr;
        }

        Iterator& operator=(Iterator&& other)
        {
            /// Decrease current counter before reassignment
            decreaseCounter();

            m_ptr = other.m_ptr;
            map = other.map;

            /// Avoid unneccesary descrease counter in other's destructor
            other.map = nullptr;

            return *this;
        }

        Iterator(const Iterator & other) = delete;
        Iterator(Iterator & other) = delete;
        Iterator& operator=(const Iterator& other) = delete;
        Iterator& operator=(Iterator& other) = delete;
 
        ~Iterator() 
        {
            decreaseCounter();
        }

        reference operator*() const { return m_ptr->value; }
        pointer operator->() { return &(m_ptr->value); }
        Iterator& operator++() { m_ptr = m_ptr->next.load(); return *this; }
        Iterator operator++(int) { Iterator tmp(m_ptr); ++(*this); return tmp; }
        friend bool operator== (const Iterator& a, const Iterator& b) { return a.m_ptr == b.m_ptr; }
        friend bool operator!= (const Iterator& a, const Iterator& b) { return a.m_ptr != b.m_ptr; }

    private:
        friend class ScanWaitFreeMap;

        inline void increaseCounter()
        {
            if (map)
            {
                map->iterator_counter++;
            }
        }
        inline void decreaseCounter()
        {
            if (map)
            {
                map->iterator_counter--;
                map->tryClearTrash();
            }
        }

        Iterator(Node* ptr) : m_ptr(ptr), map(nullptr) {}        

        Iterator(Node* ptr, ScanWaitFreeMap * map_) : m_ptr(ptr), map(map_) 
        {
            increaseCounter();
        }

        /// Get header needs to make sure add iterator_counter before load atomic next
        Iterator(ScanWaitFreeMap * map_) : map(map_) 
        {
            increaseCounter();
            m_ptr = map->head.next.load();
        }

        Node* m_ptr;
        ScanWaitFreeMap* map;
    };

    void insert(const ValuesVec & values, GetKeyFunc && getKeyFunc)
    {
        if (values.empty())
            return;
        auto lock = WriteLockHolder(this);
        NodePtr new_data_head = std::make_shared<Node>();
        NodePtr new_data_tail = std::make_shared<Node>();
        new_data_head->next.store(new_data_tail.get());
        new_data_tail->prev = new_data_head.get();
        NodePtrList current_trash_nodes;
        for (const Value& value : values)
        {
            NodePtr new_node = std::make_shared<Node>();
            new_node->value = value;
            Key key = getKeyFunc(value);
            auto it = store_map.find(key);
            if (it != store_map.end())
            {
                /// Replace existing node
                NodePtr exist_node = it->second;
                replaceNode(new_node.get(), exist_node.get());
                it->second = new_node;
                /// Need to push old node to trash to avoid on-going read operator hit released object
                current_trash_nodes.push_back(std::move(exist_node));
            }
            else
            {
                addNode(new_node.get(), new_data_head.get());
                store_map.emplace(std::move(key), std::move(new_node));
            }
        }


        /// Put newly add nodes to the list at once
        /// Make sure they are visible to scan at the same time
        if (new_data_tail->prev != new_data_head.get())
        {
            /// First make sure the new list's end connect to the head's next
            auto * data_rbegin = new_data_tail->prev;
            auto * head_next = head.next.load();
            data_rbegin->next.store(head_next);
            head_next->prev = data_rbegin;
            /// Second connect head with the new data's begin
            auto * data_begin = new_data_head->next.load();
            data_begin->prev = &head;
            head.next.store(data_begin);
        }

        handleTrash(std::move(current_trash_nodes));
        updateSize();
    }

    std::pair<Iterator, bool> emplace(const Key & key, Value && value)
    {
        auto lock = WriteLockHolder(this);
        auto it = store_map.find(key);
        if (it != store_map.end())
            return std::make_pair(Iterator(it->second.get(), this), false);
        
        NodePtr node = std::make_shared<Node>();
        auto * node_ptr = node.get();
        node->value = std::move(value);
        addNode(node.get(), &head);
        store_map.emplace(key, std::move(node));

        updateSize();

        return std::make_pair(Iterator(node_ptr, this), true);
    }

    void update(const Key & key, const Value & value)
    {
        NodePtr node = std::make_shared<Node>();
        node->value = std::move(value);
        auto lock = WriteLockHolder(this);
        auto it = store_map.find(key);
        if (it != store_map.end())
        {
            replaceNode(node.get(), it->second.get());
            NodePtrList current_trash_nodes{it->second};
            it->second = node;
            handleTrash(std::move(current_trash_nodes));
            return;
        }
        addNode(node.get(), &head);
        store_map.emplace(key, std::move(node));
        updateSize();
    }

    Iterator find(const Key & key)
    {
        std::shared_lock lock(mutex);
        auto it = store_map.find(key);
        if (it != store_map.end())
            return Iterator(it->second.get(), this);
        return end();
    }

    size_t erase(const Key & key)
    {
        auto lock = WriteLockHolder(this);
        auto it = store_map.find(key);
        if (it == store_map.end())
            return 0;
        removeNode(it->second.get());
        NodePtrList current_trash_nodes{it->second};
        store_map.erase(it);
        handleTrash(std::move(current_trash_nodes));
        updateSize();
        return 1;
    }

    size_t erase(const KeysVec & keys)
    {
        if (keys.empty())
            return 0;
        size_t erase_items{0};
        NodePtrList current_trash_nodes;
        auto lock = WriteLockHolder(this);
        for (const auto key : keys)
        {
            auto it = store_map.find(key);
            if (it == store_map.end())
                continue;
            removeNode(it->second.get());
            current_trash_nodes.push_back(it->second);
            store_map.erase(it);
            ++erase_items;
        }
        handleTrash(std::move(current_trash_nodes));
        updateSize();
        return erase_items;
    }

    template<typename Pred>
    size_t erase_if(const KeysVec & keys, Pred pred)
    {
        if (keys.empty())
            return 0;
        size_t erase_items{0};
        NodePtrList current_trash_nodes;
        auto lock = WriteLockHolder(this);
        for (const auto key : keys)
        {
            auto it = store_map.find(key);
            if (it == store_map.end() || !pred(it->second->value))
                continue;
            removeNode(it->second.get());
            current_trash_nodes.push_back(it->second);
            store_map.erase(it);
            ++erase_items;
        }
        handleTrash(std::move(current_trash_nodes));
        updateSize();
        return erase_items;
    }

    void clear()
    {
        auto lock = WriteLockHolder(this);
        NodePtrList current_trash_nodes;
        for (const auto & [k,v] : store_map)
            current_trash_nodes.push_back(v);
        tail.prev = &head;
        head.next.store(&tail);
        store_map.clear();

        handleTrash(std::move(current_trash_nodes));
        updateSize();
    }

    size_t size() const { return _size.load(); }

    size_t trashSize() const { return trash_size.load(); }

    Iterator begin() { return Iterator(this); }
    Iterator end() { return Iterator(&tail); }

    ScanWaitFreeMap(const Delay & trash_lifetime_seconds = Delay(60*60))
        :_trash_lifetime_seconds(trash_lifetime_seconds)
    {
        head.next.store(&tail);
        head.prev = nullptr;
        tail.prev = &head;
        tail.next = nullptr;
    }

private:
    using NodePtr = std::shared_ptr<Node>;
    using NodePtrVec = std::vector<NodePtr>;
    using NodePtrList = std::list<NodePtr>;
    using Clock = std::chrono::steady_clock;
    using Timestamp = Clock::time_point;

    friend struct Iterator;

    struct WriteLockHolder
    {
        explicit WriteLockHolder(ScanWaitFreeMap * map_)
            :map(map_), lock(map->mutex, std::defer_lock)
        {
            map->write_counter++;
            lock.lock();
        }

        ~WriteLockHolder()
        {
            map->write_counter--;
            map->clearTrash();
        }

        ScanWaitFreeMap * map;
        std::unique_lock<std::shared_mutex> lock;
    };

    void updateSize() { _size.store(store_map.size()); }

    void handleTrash(NodePtrList && current_trash_nodes)
    {
        Timestamp now = Clock::now();
        /// Clear trash by time in case always iterators, rarely happen
        for (auto it = trash_nodes.begin(); it != trash_nodes.end();)
        {
            if ((now > it->first) && (now - it->first > this->_trash_lifetime_seconds))
            {
                it = trash_nodes.erase(it);
                trash_size--;
            }
            else
                break;
        }

        /// Store current trash nodes
        if (!current_trash_nodes.empty())
        {
            trash_nodes.push_back(std::make_pair(std::move(now), std::move(current_trash_nodes)));
            trash_size++;
        }
    }

    bool clearTrash()
    {
        /// Clear all trash nodes when there is no iterator
        if (iterator_counter.load() > 0)
            return false;
        trash_nodes.clear();
        trash_size.store(0);
        return true;
    }

    void tryClearTrash()
    {
        /// Fast path when no trash
        /// or having other on-going write (prefer write action do the clear trash to reduce the impact for scan)
        /// or having other on-going iterators
        if (likely(trash_size.load(std::memory_order_consume) == 0
            || write_counter.load(std::memory_order_consume) > 0
            || iterator_counter.load(std::memory_order_consume) > 0))
            return;

        std::unique_lock lock(mutex, std::defer_lock);
        if(lock.try_lock())
            clearTrash();
    }   

    static void removeNode(Node * node)
    {
        auto * node_next = node->next.load();
        node_next->prev = node->prev;
        node_next->prev->next.store(node_next);
    }

    static void addNode(Node * node, Node * target_head)
    {
        auto * target_head_next = target_head->next.load();
        node->prev = target_head;
        target_head_next->prev = node;
        node->next.store(target_head_next);
        target_head->next.store(node);
    }

    static void replaceNode(Node * new_node, Node * exist_node)
    {
        /// Handle exist node next
        auto * exist_node_next = exist_node->next.load();
        new_node->next.store(exist_node_next);
        exist_node_next->prev = new_node;
        /// Handle exist node prev
        auto * exist_node_prev = exist_node->prev;
        new_node->prev = exist_node_prev;
        exist_node_prev->next.store(new_node);
    }

    std::shared_mutex mutex;
    std::unordered_map<Key, NodePtr> store_map;
    std::atomic<size_t> _size{0};
    Node head;
    Node tail;

    /// Record the on-going iterators
    std::atomic<uint64_t> iterator_counter{0};
    /// Record the on-going writes
    std::atomic<uint64_t> write_counter{0};
    std::atomic<uint64_t> trash_size{0};
    Delay _trash_lifetime_seconds;
    std::list<std::pair<Timestamp, NodePtrList>> trash_nodes;
};

}
