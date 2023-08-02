#include <Core/Types.h>

#define APPLY_FOR_EACH_VECTOR_TYPE(V) \
    V(UInt8   , UInt8  ) \
    V(UInt16  , UInt16 ) \
    V(UInt32  , UInt32 ) \
    V(UInt64  , UInt64 ) \
    V(UInt128 , UInt128) \
    V(Int8    , Int8   ) \
    V(Int16   , Int16  ) \
    V(Int32   , Int32  ) \
    V(Int64   , Int64  ) \
    V(Int128  , Int128 ) \
    V(Float32 , Float32) \
    V(Float64 , Float64) \
    V(UUID    , UUID   ) \
    V(Date    , UInt16 ) \
    V(DateTime, UInt32 )

