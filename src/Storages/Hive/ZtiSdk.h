#pragma once

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Weverything"

#ifndef ZTI_SDK_CPP_REMOVE_USING_NAMESPACE_STD
#    define ZTI_SDK_CPP_REMOVE_USING_NAMESPACE_STD
#    include <zti_sdk.h>
#    undef ZTI_SDK_CPP_REMOVE_USING_NAMESPACE_STD
#else
#    include <zti_sdk.h>
#endif

#pragma clang diagnostic pop
