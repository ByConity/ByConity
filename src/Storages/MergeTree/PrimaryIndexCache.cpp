#include <iterator>
#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeHelper.h>
#include <IO/VarInt.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>
#include <common/defines.h>

namespace DB
{
void PrimaryIndexCache::removeExternal(const Key & key, const MappedPtr & value, size_t size)
{
    if (nvm_cache && nvm_cache->isEnabled() && size)
    {
        auto hash_key = HybridCache::makeHashKey(&key, sizeof(Key));
        auto token = nvm_cache->createPutToken(hash_key.key());

        std::shared_ptr<Memory<>> mem = std::make_shared<Memory<>>(DBMS_DEFAULT_BUFFER_SIZE);
        BufferWithOutsideMemory<WriteBuffer> write_buffer(*mem);

        writeVarUInt(value->size(), write_buffer);
        chassert(!value->empty());
        writeVarUInt((*value)[0]->size(), write_buffer); // row size

        for (const auto & col : *value)
        {
            writeVarUInt(static_cast<UInt64>(col->getDataType()), write_buffer); // write type_index
            auto serializer = createBaseDataTypeFromTypeIndex(col->getDataType())->getDefaultSerialization();
            serializer->serializeBinaryBulk(*col, write_buffer, 0, col->size());
        }

        nvm_cache->put(
            hash_key,
            std::move(mem),
            std::move(token),
            [](void * obj) {
                auto * ptr = reinterpret_cast<Memory<> *>(obj);
                return HybridCache::BufferView{ptr->size(), reinterpret_cast<UInt8 *>(ptr->data())};
            },
            HybridCache::EngineTag::PrimaryIndexCache);
    }
}

PrimaryIndexCache::MappedPtr PrimaryIndexCache::loadExternal(const Key & key)
{
    if (nvm_cache && nvm_cache->isEnabled())
    {
        auto handle = nvm_cache->find<Mapped>(
            HybridCache::makeHashKey(&key, sizeof(Key)),
            [&key, this](std::shared_ptr<void> ptr, HybridCache::Buffer buffer) {
                auto cell = std::static_pointer_cast<Mapped>(ptr);
                auto read_buffer = buffer.asReadBuffer();

                MutableColumns cols;

                UInt64 col_cnt;
                readVarUInt(col_cnt, read_buffer);
                UInt64 row_cnt;
                readVarUInt(row_cnt, read_buffer);

                cols.resize(col_cnt);

                for (UInt64 i = 0; i < col_cnt; i++)
                {
                    UInt64 type;
                    readVarUInt(type, read_buffer);
                    auto data_type = createBaseDataTypeFromTypeIndex(static_cast<TypeIndex>(type));
                    cols[i] = data_type->createColumn();
                    cols[i]->reserve(row_cnt);
                    auto serializer = data_type->getDefaultSerialization();
                    serializer->deserializeBinaryBulk(*cols[i], read_buffer, row_cnt, 0, false);
                }

                cell->assign(std::make_move_iterator(cols.begin()), std::make_move_iterator(cols.end()));

                setInternal(key, cell, true);
            },
            HybridCache::EngineTag::PrimaryIndexCache);
        if (auto ptr = handle.get())
        {
            auto mapped = std::static_pointer_cast<Mapped>(ptr);
            if (!mapped->empty())
                return mapped;
        }
    }
    return nullptr;
}
}
