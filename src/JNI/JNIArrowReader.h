#pragma once

#include "JNIArrowStream.h"
#include "JNIByteCreatable.h"

namespace DB
{

class JNIArrowReader : public JNIByteCreatable {
public:
    JNIArrowReader(const std::string &full_classname, const std::string & pb_message);
    ~JNIArrowReader() override;

    const ArrowSchema & getSchema() { return schema; }

    void initStream();
    bool next(ArrowArray & chunk);
    void close();

private:
    jmethodID method_init_stream;

    ArrowSchema schema;
    ArrowArrayStream stream;
};
}
