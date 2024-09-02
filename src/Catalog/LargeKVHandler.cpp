#include <Catalog/LargeKVHandler.h>
#include <Catalog/MetastoreProxy.h>
#include <Common/SipHash.h>
#include <common/logger_useful.h>
#include <Poco/SHA1Engine.h>
#include <boost/algorithm/hex.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace Catalog
{

// Using SHA-1 value of the KV as its UUID so that we can perform CAS based on it.
String getUUIDForLargeKV(const String & key, const String & value)
{
    Poco::SHA1Engine engine;
    engine.update(key.data(), key.size());
    engine.update(value.data(), value.size());
    const std::vector<unsigned char> & sha1_value = engine.digest();
    String hexed_hash;
    hexed_hash.resize(sha1_value.size() * 2);
    boost::algorithm::hex(sha1_value.begin(), sha1_value.end(), hexed_hash.data());
    return hexed_hash;
}

bool tryParseLargeKVMetaModel(const String & serialized, Protos::DataModelLargeKVMeta & model)
{
    if (serialized.compare(0, 4, MAGIC_NUMBER) == 0)
        return model.ParseFromArray(serialized.c_str() + 4, serialized.size()-4);

    return false;
}

void tryGetLargeValue(const std::shared_ptr<IMetaStore> & metastore, const String & name_space, const String & key, String & value)
{
    Protos::DataModelLargeKVMeta large_kv_model;

    if (!tryParseLargeKVMetaModel(value, large_kv_model))
        return;

    String kv_id = large_kv_model.uuid();
    UInt32 subkv_number = large_kv_model.subkv_number();

    String resolved;

    if (large_kv_model.has_value_size())
        resolved.reserve(large_kv_model.value_size());

    if (subkv_number < 10)
    {
        std::vector<String> request_keys(subkv_number);
        for (size_t i=0; i<subkv_number; i++)
            request_keys[i] = MetastoreProxy::largeKVDataKey(name_space, kv_id, i);
        
        const auto & sub_values = metastore->multiGet(request_keys);
        for (const auto & [subvalue, _] : sub_values)
            resolved += subvalue;
    }
    else
    {
        auto it = metastore->getByPrefix(MetastoreProxy::largeKVDataPrefix(name_space, kv_id));
        while (it->next())
            resolved += it->value();
    }

    //check kv uuid(KV hash) to verity the data integrity
    if (getUUIDForLargeKV(key, resolved) != kv_id)
        throw Exception(fmt::format("Cannot resolve value of big KV. Data may be corrupted. Origin value size : {}, resolved size : {}"
            , large_kv_model.value_size(), resolved.size()), ErrorCodes::CORRUPTED_DATA);

    value.swap(resolved); 
}

LargeKVWrapperPtr tryGetLargeKVWrapper(
    const std::shared_ptr<IMetaStore> & metastore,
    const String & name_space,
    const String & key,
    const String & value,
    bool if_not_exists,
    const String & expected)
{
    const size_t max_allowed_kv_size = metastore->getMaxKVSize();
    size_t value_size = value.size();

    if (value_size > max_allowed_kv_size)
    {
        String large_kv_id = getUUIDForLargeKV(key, value);

        std::vector<SinglePutRequest> puts;
        UInt64 sub_key_index = 0;
        // split serialized data to make substrings match the KV size limitation
        for (size_t i=0; i<value_size; i+=max_allowed_kv_size)
        {
            puts.emplace_back(SinglePutRequest(MetastoreProxy::largeKVDataKey(name_space, large_kv_id, sub_key_index++),
                value.substr(i, max_allowed_kv_size)));
        }

        size_t subkv_number = puts.size();

        // Write an additional kv reference for GC. Use CAS to aoivd id conflict (Although it unlikely happens)
        puts.emplace_back(SinglePutRequest(MetastoreProxy::largeKVReferenceKey(name_space, large_kv_id), key, true));

        Protos::DataModelLargeKVMeta large_kv_meta;
        large_kv_meta.set_uuid(large_kv_id);
        large_kv_meta.set_subkv_number(subkv_number);
        large_kv_meta.set_value_size(value_size);

        SinglePutRequest base_req(key, MAGIC_NUMBER + large_kv_meta.SerializeAsString());
        base_req.if_not_exists = if_not_exists;
        if (!expected.empty())
        {
            // if expected value size exceed max kv size, construct DataModelLargeKVMeta to perform CAS.
            if (expected.size() > max_allowed_kv_size)
            {
                Protos::DataModelLargeKVMeta expected_large_kv_model;
                expected_large_kv_model.set_uuid(getUUIDForLargeKV(key, expected));
                expected_large_kv_model.set_subkv_number(1 + ((expected.size() - 1) / max_allowed_kv_size));
                expected_large_kv_model.set_value_size(expected.size());

                base_req.expected_value = MAGIC_NUMBER + expected_large_kv_model.SerializeAsString();
            }
            else
                base_req.expected_value = expected;
        }

        LargeKVWrapperPtr wrapper = std::make_shared<LargeKVWrapper>(std::move(base_req));
        wrapper->sub_requests.swap(puts);

        return wrapper;
    }
    else
    {
        SinglePutRequest base_req(key, value);
        base_req.if_not_exists = if_not_exists;
        if (!expected.empty())
            base_req.expected_value = expected;
        LargeKVWrapperPtr wrapper = std::make_shared<LargeKVWrapper>(std::move(base_req));
        return wrapper;
    }
}

void addPotentialLargeKVToBatchwrite(
    const std::shared_ptr<IMetaStore> & metastore,
    BatchCommitRequest & batch_request,
    const String & name_space,
    const String & key,
    const String & value,
    bool if_not_eixts,
    const String & expected)
{
    LargeKVWrapperPtr largekv_wrapper = tryGetLargeKVWrapper(metastore, name_space, key, value, if_not_eixts, expected);

    for (auto & sub_req : largekv_wrapper->sub_requests)
        batch_request.AddPut(sub_req);

    batch_request.AddPut(largekv_wrapper->base_request);
}

}

}
