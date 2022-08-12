#include <Parsers/ASTTableColumnReference.h>

#include <Storages/IStorage.h>
#include <IO/WriteHelpers.h>

namespace DB
{

static inline String formatStorageName(const StoragePtr & storage, const String & column_name, char delim = '.')
{
    return storage->getStorageID().getFullTableName() + delim + std::to_string(reinterpret_cast<size_t>(storage.get()))
        + delim + column_name;
}

String ASTTableColumnReference::getID(char delim) const
{
    return std::string("TableColumnRef") + delim + formatStorageName(storage, column_name, delim);
}

void ASTTableColumnReference::appendColumnName(WriteBuffer & buffer) const
{
    writeString(getID('.'), buffer);
}
}
