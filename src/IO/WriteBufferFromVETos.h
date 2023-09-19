#pragma once

#include <Common/config.h>

#if USE_VE_TOS

#include <common/logger_useful.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/VETosCommon.h>
#include <memory>
#include <string>
#include <Poco/URI.h>
#include <TosClientV2.h>
#include <model/bucket/CreateBucketV2Input.h>
#include <model/object/CompleteMultipartUploadInput.h>
#include <model/object/UploadPartV2Input.h>

namespace TOS = VolcengineTos ; 

namespace DB
{
class WriteBufferFromVETos : public WriteBufferFromFileBase
{
private:
    VETosConnectionParams vetos_connect_params;
    std::string bucket;
    std::string key;
    std::unique_ptr<TOS::TosClientV2> client_ptr; 
    TOS::Outcome<TOS::TosError, TOS::CreateMultipartUploadOutput> upload;
    std::vector<TOS::UploadedPartV2> uploaded_parts;

    bool finalized ;

public:
    //Buffer size must bigger than VE_TOS_DEFAULT_BUFFER_SZIE = 5 MB
    explicit WriteBufferFromVETos(
        const VETosConnectionParams& vetos_connect_params,
        const std::string& bucket,
        const std::string& key,
        size_t buffer_size_ = VE_TOS_DEFAULT_BUFFER_SZIE
    );

    void nextImpl() override;
    void sync() override;

    ~WriteBufferFromVETos() override;

    std::string getFileName() const override;
    
    void finalize() override;
};
}

#endif
