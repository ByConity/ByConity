#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <IO/S3Common.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <boost/algorithm/string.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/program_options.hpp>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>

#define MAX_OBJECT_SIZE 5368709120

using namespace Aws;

bool CopyObject(const String &fromBucket, const String &fromKey, const String &toBucket, 
                    const String &toKey, const S3::S3Client & client) {
    S3::Model::CopyObjectRequest request;

    request.WithCopySource(fromBucket + "/" + fromKey)
            .WithKey(toKey)
            .WithBucket(toBucket);

    S3::Model::CopyObjectOutcome outcome = client.CopyObject(request);
    if (!outcome.IsSuccess()) {
        const S3::S3Error &err = outcome.GetError();
        LOG_WARNING(&Poco::Logger::get("RenameS3Tool"), "Error: CopyObject: {}: {}", 
            err.GetExceptionName() , err.GetMessage());
    }
    else {
        LOG_INFO(&Poco::Logger::get("RenameS3Tool"), "Successfully copy {} in bucket {} to {} in bucket {}", 
            fromKey, fromBucket, toKey, toBucket);
    }

    return outcome.IsSuccess();
}

bool UploadPartCopy(const String &fromBucket, const String &fromKey, const String &toBucket, 
                    const String &toKey, const int64_t & objectSize, const S3::S3Client & client) {
    // Initiate the multipart upload.
    S3::Model::CreateMultipartUploadRequest init_request;
    init_request.WithBucket(toBucket).WithKey(toKey);
    S3::Model::CreateMultipartUploadOutcome init_outcome = client.CreateMultipartUpload(init_request);

    if (!init_outcome.IsSuccess()) {
        const S3::S3Error &err = init_outcome.GetError();
        std::cout << "Error: CopyObject: " << err.GetExceptionName() << err.GetMessage() << std::endl;
        LOG_WARNING(&Poco::Logger::get("RenameS3Tool"), "Error: CopyObject: {}: {}", 
            err.GetExceptionName() , err.GetMessage());
        return false;
    }

    // Copy the objec tusing 1GB parts.
    int64_t part_size = 1024 * 1024 * 1024;
    int64_t byte_position = 0;
    int16_t part_num = 1;
    S3::Model::CompletedMultipartUpload completed_multipart;
    while (byte_position < objectSize) {
        // The last part might be smaller than partSize, so check to make sure
        // that lastByte isn't beyond the end of the object.
        int64_t last_byte = std::min(byte_position + part_size - 1, objectSize - 1);
        String source_range = "bytes=" + std::to_string(byte_position) + "-" + std::to_string(last_byte);

        // Copy this part.
        S3::Model::UploadPartCopyRequest copy_request;
        copy_request.WithCopySource(fromBucket + "/" + fromKey)
                .WithBucket(toBucket)
                .WithKey(toKey)
                .WithUploadId(init_outcome.GetResult().GetUploadId())
                .WithCopySourceRange(source_range)
                .WithPartNumber(part_num);
        S3::Model::UploadPartCopyOutcome copy_outcome = client.UploadPartCopy(copy_request);
        if (!copy_outcome.IsSuccess()) {
            const S3::S3Error &err = init_outcome.GetError();
            std::cout << "Error: CopyObject: " << err.GetExceptionName() << err.GetMessage() << std::endl;
            LOG_WARNING(&Poco::Logger::get("RenameS3Tool"), "Error: CopyObject: {}: {}", 
                err.GetExceptionName() , err.GetMessage());
            return false;
        }
        S3::Model::CompletedPart completed_part;
        completed_part.WithPartNumber(part_num).WithETag(copy_outcome.GetResult().GetCopyPartResult().GetETag());
        completed_multipart.AddParts(completed_part);
        byte_position += part_size;
        part_num++;
    }

    // Complete the upload request to concatenate all uploaded parts and make the copied object available.
    S3::Model::CompleteMultipartUploadRequest complete_request;
    complete_request.WithBucket(toBucket)
                    .WithKey(toKey)
                    .WithUploadId(init_outcome.GetResult().GetUploadId())
                    .WithMultipartUpload(completed_multipart);
    auto complete_outcome = client.CompleteMultipartUpload(complete_request);

    if (!complete_outcome.IsSuccess()) {
        const S3::S3Error &err = complete_outcome.GetError();
        std::cout << "Error: CopyObject: " << err.GetExceptionName() << err.GetMessage() << std::endl;
        LOG_WARNING(&Poco::Logger::get("RenameS3Tool"), "Error: CopyObject: {}: {}", 
            err.GetExceptionName() , err.GetMessage());
    }
    else {
        LOG_INFO(&Poco::Logger::get("RenameS3Tool"), "Successfully copy {} in bucket {} to {} in bucket {}", 
            fromKey, fromBucket, toKey, toBucket);
    }

    return complete_outcome.IsSuccess();
}

bool DeleteObject(const String &objectKey, const String &fromBucket, const S3::S3Client & client) {
    S3::Model::DeleteObjectRequest request;
    request.WithKey(objectKey)
            .WithBucket(fromBucket);

    S3::Model::DeleteObjectOutcome outcome = client.DeleteObject(request);

    if (!outcome.IsSuccess()) {
        const S3::S3Error &err = outcome.GetError();
        LOG_WARNING(&Poco::Logger::get("RenameS3Tool"), "Error: DeleteObject: {}: {}", 
            err.GetExceptionName() , err.GetMessage());
    }
    else {
        LOG_INFO(&Poco::Logger::get("RenameS3Tool"), "Successfully deleted the object {}.", objectKey);
    }

    return outcome.IsSuccess();
}

bool ListAndRenameObjects(const String &fromBucket, const String &rootPrefix, const String& toBucket,
                const bool needDelete, const bool checkUUid, const S3::S3Client &client, const int threadNum) {
    S3::Model::ListObjectsRequest request;
    request.WithBucket(fromBucket);
    if (!rootPrefix.empty()) {
        request.WithPrefix(rootPrefix);
    }

    boost::asio::thread_pool thread_pool(threadNum);
    bool truncated = true;
    while (truncated) {
        auto outcome = client.ListObjects(request);
        if (!outcome.IsSuccess()) {
            const S3::S3Error &err = outcome.GetError();
            LOG_WARNING(&Poco::Logger::get("RenameS3Tool"), "Error: ListObjects: {}: {}", 
                err.GetExceptionName() , err.GetMessage());
            return false;
        }
        else {
            Vector<S3::Model::Object> objects = outcome.GetResult().GetContents();
            for (S3::Model::Object &object: objects) {
                // root_prefix/table_uuid/part_uuid/name
                String from_key = object.GetKey();
                Vector<String> split_parts;
                boost::split(split_parts, from_key, boost::is_any_of("/"));

                if (split_parts.size() == 4) {
                    if (checkUUid) {
                        DB::UUID uuid;
                        DB::ReadBufferFromString read_buffer(split_parts[1]);
                        if (!DB::tryReadUUIDText(uuid, read_buffer)) {
                            continue;
                        }
                    }
                    String to_key = split_parts[0] + "/" + split_parts[2] + "/" + split_parts[3];
                    int64_t object_size = object.GetSize();
                    // submit copy task
                    boost::asio::post(thread_pool, [=, &client]() {
                        bool copy_result = false;
                        if (object_size >= MAX_OBJECT_SIZE) {
                            copy_result = UploadPartCopy(fromBucket, from_key, toBucket, to_key, object_size, client);
                        } else {
                            copy_result = CopyObject(fromBucket, from_key, toBucket, to_key, client); 
                        }
                        if (copy_result && needDelete) {
                            DeleteObject(from_key, fromBucket, client);
                        }
                    });
                }
            }
            if (objects.size() == request.GetMaxKeys()) {
                request.SetMarker(objects[objects.size() - 1].GetKey());
            } else {
                truncated = false;
                break;
            }
        }
    }
    thread_pool.join();
    return true;
    
}

namespace po = boost::program_options;
/**
 * @brief This tool is used to delete table_uuid from the key of s3 objects. Only patterns like 
 *          "root_prefix/table_uuid/part_uuid/key" will be renamed, other objects will keep like before.
 */
int mainEntryClickhouseS3RenameTool(int argc, char ** argv)
{
    po::options_description desc("S3 rename tools is used to delete table_uuid from the key of s3 objects. Parameters");

    desc.add_options()
        ("help", "produce help message")
        ("s3_ak_id", po::value<String>(), "access key id of S3")
        ("s3_ak_secret", po::value<String>(), "secret access key id of S3")
        ("s3_region", po::value<String>(), "region of S3")
        ("s3_endpoint", po::value<String>(), "endpoint of S3")
        ("from_bucket", po::value<String>(), "bucket name in which files need to be renamed")
        ("root_prefix", po::value<String>()->default_value(""), "files that need to be renamed start with. If not specified, all files in bucket will be renamed")
        ("to_bucket", po::value<String>(), "bucket name that files need to be moved to, default from_bucket")
        ("thread_number", po::value<int>()->default_value(1), "using how many threads, default 1")
        ("need_delete", po::value<bool>()->default_value(true), "whether delete origin file, default true")
        ("uuid_check", po::value<bool>()->default_value(true), "whether check uuid is valid or not, default true")
        ("enable_logging", "Enable logging output")
        ("logging_level", po::value<String>()->default_value("info"), "logging level")
    ;

    po::variables_map vm;
    po::parsed_options parsed_opts = po::command_line_parser(argc, argv)
        .options(desc)
        .run();

    po::store(parsed_opts, vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }

    if (!vm.count("s3_ak_id") || !vm.count("s3_ak_secret") || !vm.count("s3_endpoint") 
            || !vm.count("from_bucket") || !vm.count("s3_region")) {
        std::cerr << "Missing required field\n" << desc << std::endl;
        return 1;
    }

    if (vm.count("enable_logging"))
    {
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
        Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
        Poco::Logger::root().setLevel(vm["logging_level"].as<String>());
        Poco::Logger::root().setChannel(channel);
    }

    DB::S3::S3Config s3_config(vm["s3_endpoint"].as<String>(), vm["s3_region"].as<String>(), 
                                vm["from_bucket"].as<String>(), vm["s3_ak_id"].as<String>(), 
                                vm["s3_ak_secret"].as<String>(), vm["root_prefix"].as<String>());

    String to_bucket = vm["from_bucket"].as<String>();
    if (vm.count("to_bucket")) {
        to_bucket = vm["to_bucket"].as<String>();
    }

    ListAndRenameObjects(
        vm["from_bucket"].as<String>(), 
        vm["root_prefix"].as<String>(), 
        to_bucket,
        vm["need_delete"].as<bool>(), 
        vm["uuid_check"].as<bool>(), 
        *s3_config.create(), 
        vm["thread_number"].as<int>());
    
    return 1;
}
