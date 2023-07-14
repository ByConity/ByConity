#include <iostream>
#include <string>
#include <IO/S3Common.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <boost/algorithm/string.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/program_options.hpp>

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
        std::cerr << "Error: CopyObject: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;

    }
    else {
        std::cout << "Successfully copy " << fromKey << " in bucket " << fromBucket 
            <<  " to " << toKey << " in bucket " << toBucket << "." << std::endl;
    }

    return outcome.IsSuccess();
}

bool DeleteObject(const String &objectKey, const String &fromBucket, const S3::S3Client & client) {
    S3::Model::DeleteObjectRequest request;
    request.WithKey(objectKey)
            .WithBucket(fromBucket);

    S3::Model::DeleteObjectOutcome outcome = client.DeleteObject(request);

    if (!outcome.IsSuccess()) {
        const S3::S3Error &err = outcome.GetError();
        std::cerr << "Error: DeleteObject: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "Successfully deleted the object." << objectKey << std::endl;
    }

    return outcome.IsSuccess();
}

bool ListAndRenameObjects(const String &fromBucket, const String &rootPrefix, const String& toBucket,
                const bool needDelete, const S3::S3Client &client, const int threadNum) {
    S3::Model::ListObjectsRequest request;
    request.WithBucket(fromBucket);
    if (!rootPrefix.empty()) {
        request.WithPrefix(rootPrefix);
    }

    auto outcome = client.ListObjects(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: ListObjects: " <<
                  outcome.GetError().GetMessage() << std::endl;
        return false;
    }
    else {
        Vector<S3::Model::Object> objects = outcome.GetResult().GetContents();
        boost::asio::thread_pool thread_pool(threadNum);
        for (S3::Model::Object &object: objects) {
            // root_prefix/table_uuid/part_uuid/name
            String from_key = object.GetKey();
            std::cout << "Get object " + from_key << std::endl;
            Vector<String> split_parts;
            boost::split(split_parts, from_key, boost::is_any_of("/"));

            if (split_parts.size() == 4) {
                String to_key = split_parts[0] + "/" + split_parts[2] + "/" + split_parts[3];
                boost::asio::post(thread_pool, [=, &client]() {
                    bool copy_result = CopyObject(fromBucket, from_key, toBucket, to_key, client); 
                    if (copy_result && needDelete) {
                        DeleteObject(from_key, fromBucket, client);
                    }
                });
            }
        }
        thread_pool.join();
        return true;
    }
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
        ("need_delete", po::value<bool>()->default_value(true), "whether delete origin file, default true.")
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
        *s3_config.create(), 
        vm["thread_number"].as<int>());
    
    return 1;
}
