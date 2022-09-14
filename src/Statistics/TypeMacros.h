#pragma once

#define INTEGER_TYPE_ITERATE(X_) \
    X_(Int8) \
    X_(Int16) \
    X_(Int32) \
    X_(Int64) \
    X_(UInt8) \
    X_(UInt16) \
    X_(UInt32) \
    X_(UInt64) \
    X_(UInt128) \
    X_(UInt256) \
    X_(Int128) \
    X_(Int256)

#define FLOATING_TYPE_ITERATE(X_) \
    X_(Float64) \
    X_(Float32)

#define SPECIAL_TYPE_ITERATE(X_) \
    /* other types */ \
    X_(String)

#define FIXED_TYPE_ITERATE(X_) \
    INTEGER_TYPE_ITERATE(X_) \
    FLOATING_TYPE_ITERATE(X_)

#define ALL_TYPE_ITERATE(X_) \
    INTEGER_TYPE_ITERATE(X_) \
    FLOATING_TYPE_ITERATE(X_) \
    SPECIAL_TYPE_ITERATE(X_)

#define TYPED_STATS_ITERATE(X_) \
    X_(KllSketch) \
    X_(NdvBuckets) \
    X_(NdvBucketsExtend) \
    X_(NdvBucketsResult)

#define UNTYPED_STATS_ITERATE(X_) \
    X_(DummyAlpha) \
    X_(DummyBeta) \
    X_(TableBasic) \
    X_(ColumnBasic) \
    X_(CpcSketch)
