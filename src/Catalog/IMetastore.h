#pragma once

#include <memory>
#include <string_view>
#include <common/StringRef.h>
#include <Catalog/IMultiWrite.h>

namespace DB
{

namespace Catalog
{

#define MAX_BATCH_SIZE 1024
#define DEFAULT_SCAN_BATCH_COUNT 10000

    struct WriteRequest
    {
        std::string_view key;
        std::optional<std::string_view> old_value;
        std::string_view new_value;
        bool if_not_exists{false};
        std::function<void(int, const std::string &)> callback;
    };
    using WriteRequests = std::vector<WriteRequest>;

    enum CatalogCode : int
    {
        OK = 0,
        KEY_NOT_FOUND = -10002,
        CAS_FAILED = -10003,
    };

    struct WriteResponse
    {
        int code{0};
        uint64_t current_version{0};
        std::string current_value;
    };
    using WriteResponses = std::vector<WriteResponse>;

/***
* Interface for metastore implementation.
*/
    class IMetaStore
    {
    public:
        /***
         * Iterator to go over a set of record in metastore.
         */
        struct Iterator{
            virtual ~Iterator() {}
            virtual bool next() = 0;
            virtual String key() = 0;
            virtual String value() = 0;
        };

        using IteratorPtr = std::shared_ptr<Iterator>;
        IMetaStore(){}
        virtual ~IMetaStore() {}

        /***
         * Save a record into metastore;
         */
        virtual void put(const String &, const String &, bool if_not_exists = false) = 0;

        /***
         * Put with CAS. Return true if CAS succeed, otherwise return false.
         */
        virtual bool putCAS(const String &, const String &, const String &) = 0;

        virtual std::pair<bool, String> putCASWithOldValue(const String & key, const String & value, const String & expected) = 0;
        /***
         * Get a record by name from metastore;
         */
        virtual uint64_t get(const String &, String &) = 0;

        /***
         * Get a set of records by their names from metastore;
         */
        virtual std::vector<std::pair<String, UInt64>> multiGet(const std::vector<String> &) = 0;

        /***
         * Put records with CAS, get current value if CAS failed;
         */
        virtual void multiPutCAS(const Strings &, const String &, const Strings &, bool if_not_exists, std::vector<std::pair<uint32_t , String>> &)  = 0;

        virtual bool multiWriteCAS(const WriteRequests & requests) = 0;

        virtual MultiWritePtr createMultiWrite(bool with_cas = true) = 0;

        /***
         * Update a specific record in metastore;
         */
        virtual void update(const String&, const String&) = 0;

        /***
         * Delete a specific record from metastore;
         */
        virtual void drop(const String &, const UInt64 & expected = 0) = 0;

        /***
         * Get all records from metastore;
         */
        virtual IteratorPtr getAll() = 0;

        /***
         * Range scan by specific prefix; limit the number of result
         */
        virtual IteratorPtr getByPrefix(const String &, const size_t & limit = 0, uint32_t scan_batch_size = DEFAULT_SCAN_BATCH_COUNT) = 0;

        /***
         * Scan a range of records by start and end key;
         */
        virtual IteratorPtr getByRange(const String &, const String &, const bool, const bool) = 0;

        /***
         * Claer all metainfo in the metastore start with specification prefix;
         */
        virtual void clean(const String & prefix) = 0;

        /***
         * Close metastore;
         */
        virtual void close() = 0;
    };
}

}
