#pragma once

#include <brpc/server.h>


#define REGISTER_SERVICE_IMPL(service_name) \
class service_name##_RegisterService { \
public: \
    explicit service_name##_RegisterService(ContextMutablePtr global_context) { \
        service = std::make_unique<service_name>(global_context); \
    } \
    std::unique_ptr<::google::protobuf::Service> service; \
}

