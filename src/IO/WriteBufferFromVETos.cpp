#include <Common/config.h>

#if USE_VE_TOS

#include <IO/WriteBufferFromVETos.h> 
#include <IO/VETosCommon.h>
#include <vector>
#include <iostream>
#include <TosClientV2.h>
#include <model/bucket/CreateBucketV2Input.h>
#include <model/object/UploadedPart.h>
#include <model/object/UploadPartV2Input.h>
#include <model/object/CompleteMultipartUploadInput.h>
#include <model/object/CreateMultipartUploadInput.h>
#include <auth/StaticCredentials.h>

namespace TOS =  VolcengineTos ;

namespace DB
{

namespace ErrorCodes
{
    extern const int VETOS_CREATE_UPLOAD_ERROR;
    extern const int VETOS_UPLOAD_PART_ERROR;
    extern const int VETOS_SUBMIT_UPLOAD_ERROR;
    extern const int NOT_IMPLEMENTED;
}

    WriteBufferFromVETos::WriteBufferFromVETos(
        const VETosConnectionParams& vetos_connect_params_,
        const std::string& bucket_,
        const std::string& key_,
        size_t buffer_size_
    ) : WriteBufferFromFileBase(buffer_size_,nullptr,0)
      , vetos_connect_params(vetos_connect_params_)
      , bucket(bucket_)
      , key(key_)
    {   
        client_ptr = vetos_connect_params.getVETosClient();
        
        uploaded_parts.clear();
    
        TOS::CreateMultipartUploadInput input(bucket,key);
        upload = client_ptr->createMultipartUpload(input);
        finalized = false;
        
        if(!upload.isSuccess())
        {
            // ERROR OUTPUT
            std::string error_str("WriteBufferFromVETos::WriteBufferFromVETos()");
            throw Exception(error_str+upload.error().String(),ErrorCodes::VETOS_CREATE_UPLOAD_ERROR);
        }
    }

    void WriteBufferFromVETos::nextImpl()
    {
        if (!offset())
        {
            return;
        }

        std::string tmp_string(working_buffer.begin(), offset());
        auto output_stream = std::make_shared<std::stringstream>(std::move(tmp_string));
        TOS::UploadPartV2Input input(
            bucket, key, upload.result().getUploadId(), output_stream->tellg(), uploaded_parts.size() + 1, output_stream);
        auto part = client_ptr->uploadPart(input);
        if (!part.isSuccess())
        {
            //ERROR OUTPUT
            std::string error_str("WriteBufferFromVETos::nextImpl()");
            throw Exception(error_str + part.error().String(), ErrorCodes::VETOS_UPLOAD_PART_ERROR);
        }
        else
        {
            auto part_v2 = TOS::UploadedPartV2(part.result().getPartNumber(), part.result().getETag());
            uploaded_parts.push_back(part_v2);
        }
    }

    void WriteBufferFromVETos::sync()
    {
        next();
        if (uploaded_parts.empty())
            return;
        TOS::CompleteMultipartUploadV2Input input_complete(bucket, key, upload.result().getUploadId(), uploaded_parts);
        auto complete = client_ptr->completeMultipartUpload(input_complete);
        if (!complete.isSuccess())
        {
            TOS::AbortMultipartUploadInput abort_input(bucket, key, upload.result().getUploadId());
            auto abort = client_ptr->abortMultipartUpload(abort_input);
            if (!abort.isSuccess())
            {
                throw Exception(
                    "WriteBufferFromVETos::sync submmit and abort upload error " + abort.error().String(),
                    ErrorCodes::VETOS_SUBMIT_UPLOAD_ERROR);
            }
            //ERROR OUTPUT
            std::string error_str("WriteBufferFromVETos::sync submmit upload error ");
            throw Exception(error_str + complete.error().String(), ErrorCodes::VETOS_SUBMIT_UPLOAD_ERROR);
        }
        finalized = true;
    }

    void  WriteBufferFromVETos::finalize()
    {
        if (!finalized)
        {
            sync();
        }
    }

    std::string WriteBufferFromVETos::getFileName() const
    {
        return key;
    }
    
    WriteBufferFromVETos::~WriteBufferFromVETos()
    {
        try
        {   
            if (!finalized)
            {
                sync();
            }
            
        }
        catch(...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    } 
}

#endif
