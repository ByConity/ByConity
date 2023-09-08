#pragma once

#include <JNIByteCreatable.h>

namespace DB
{

class JNIMetaClient : public JNIByteCreatable
{
public:
    JNIMetaClient(const std::string & full_classname, const std::string & pb_message);
    ~JNIMetaClient() override = default;

    std::string getTable();
    std::string getPartitionPaths();
    std::string getFilesInPartition(const std::string &partition_path);
};
}
