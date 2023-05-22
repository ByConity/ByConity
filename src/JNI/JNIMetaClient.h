#pragma once

#include <JNIByteCreatable.h>

namespace DB
{

class JNIMetaClient : public JNIByteCreatable
{
public:
    JNIMetaClient(const std::string &full_classname, const std::string & pb_message);
    ~JNIMetaClient() override = default;

    std::string getTable(const std::string &tablename);

private:
    jmethodID method_get_table;
};
}
