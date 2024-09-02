#include <Storages/DiskCache/HashKey.h>
#include <Common/Crc32c.h>
#include <Functions/FunctionsHashing.h>

namespace DB::HybridCache
{
UInt64 hashBuffer(BufferView key, UInt64 seed)
{
    return MurmurHash3Impl64WithSeed::apply(reinterpret_cast<const char *>(key.data()), key.size(), seed);
}

UInt32 checksum(BufferView data, UInt32 init)
{
    return CRC32C::Extend(init, reinterpret_cast<const char *>(data.data()), data.size());
}
    
UInt64 HashedKey::hashBuffer(StringRef key, UInt64 seed) 
{ 
    return MurmurHash3Impl64WithSeed::apply(key.data, key.size, seed); 
}

}
