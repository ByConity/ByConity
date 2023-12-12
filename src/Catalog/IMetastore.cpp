#include <Catalog/IMetastore.h>

namespace DB
{

namespace Catalog
{

void IMetaStore::adaptiveBatchWrite(const BatchCommitRequest & req)
{
    uint32_t max_batch_size = getMaxBatchSize();
    if (req.size() < max_batch_size)
    {
        BatchCommitResponse response;
        batchWrite(req, response);
        return;
    }

    // too large request, split into smaller ones. 
    BatchCommitRequest batch_write(false);
    auto commit_rquest = [&]()
    {
        BatchCommitResponse resp;
        batchWrite(batch_write, resp);
        batch_write = {};
    };

    for (const auto & del_req : batch_write.deletes)
    {
        if (del_req.size() + batch_write.size() >= max_batch_size)
            commit_rquest();
        batch_write.AddDelete(del_req);
    }

    for (const auto & put_req: batch_write.puts)
    {
        if (put_req.size() + batch_write.size() >= max_batch_size)
            commit_rquest();
        batch_write.AddPut(put_req);
    }

    if (!batch_write.isEmpty())
        commit_rquest();
}

}

}
