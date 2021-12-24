#include <assert.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/DeleteBitmapCache.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    struct CachedBitmap
    {
        CachedBitmap(DeleteBitmapPtr bitmap_) : bitmap(std::move(bitmap_)) { }
        DeleteBitmapPtr bitmap;
    };

    void DeleteCachedBitmap([[maybe_unused]] const Slice & key, void * value)
    {
        auto * cached_bitmap = reinterpret_cast<CachedBitmap *>(value);
        delete cached_bitmap;
    }
} /// anonymous namespace

DeleteBitmapCache::DeleteBitmapCache(size_t max_size_in_bytes) : cache(IndexFile::NewLRUCache(max_size_in_bytes))
{
}

DeleteBitmapCache::~DeleteBitmapCache() = default;

void DeleteBitmapCache::insert(const String & key, DeleteBitmapPtr bitmap)
{
    if (bitmap == nullptr)
        throw Exception("bitmap is null", ErrorCodes::BAD_ARGUMENTS);

    /// TODO improve accuracy of memory usage estimation
    /// Currently we estimate the memory usage of the bitmap to be 2 bytes per elements,
    /// which may be underestimated for sparse bitmap and overestimated for dense bitmap.
    size_t charge = sizeof(CachedBitmap) + 2 * (bitmap->cardinality());
    CachedBitmap * value = new CachedBitmap(std::move(bitmap));
    auto * handle = cache->Insert(key, value, charge, &DeleteCachedBitmap);
    if (handle)
        cache->Release(handle);
    else
        delete value; /// insert failed
}

bool DeleteBitmapCache::lookup(const String & key, DeleteBitmapPtr & out_bitmap)
{
    auto * handle = cache->Lookup(key);
    if (!handle)
        return false;
    auto * value = reinterpret_cast<CachedBitmap *>(cache->Value(handle));
    out_bitmap = value->bitmap;
    cache->Release(handle);
    return true;
}

void DeleteBitmapCache::erase(const String & key)
{
    cache->Erase(key);
}

}
