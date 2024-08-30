#pragma once

#include <Catalog/MetastoreCommon.h>
#include <Catalog/IMetastore.h>
#include <Protos/data_models.pb.h>

namespace DB
{

namespace Catalog
{

static const char * MAGIC_NUMBER = "LGKV";

struct LargeKVWrapper
{
    LargeKVWrapper(SinglePutRequest && base)
    : base_request(std::move(base))
    {
    }

    SinglePutRequest base_request;
    std::vector<SinglePutRequest> sub_requests;

    bool isLargeKV() { return sub_requests.size() > 0; }
};

using LargeKVWrapperPtr = std::shared_ptr<LargeKVWrapper>;

LargeKVWrapperPtr tryGetLargeKVWrapper(
    const std::shared_ptr<IMetaStore> & metastore,
    const String & name_space,
    const String & key,
    const String & value,
    bool if_not_exists = false,
    const String & expected = "");


bool tryParseLargeKVMetaModel(const String & serialized, Protos::DataModelLargeKVMeta & model);

void tryGetLargeValue(const std::shared_ptr<IMetaStore> & metastore, const String & name_space, const String & key, String & value);

void addPotentialLargeKVToBatchwrite(
    const std::shared_ptr<IMetaStore> & metastore,
    BatchCommitRequest & batch_request,
    const String & name_space,
    const String & key,
    const String & value,
    bool if_not_eixts = false,
    const String & expected = "");
}

}
