#pragma once
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/Context_fwd.h>
#include <unordered_map>
#include <Core/Field.h>

namespace DB
{

#define PT_RELATIVE_LOCAL_PATH "data/"

class PartToolkitBase : public WithMutableContext
{

public:
    PartToolkitBase(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    virtual void execute() = 0;

    virtual ~PartToolkitBase();

protected:
    void applySettings();

    StoragePtr getTable();

    PartNamesWithDisks collectPartsFromSource(const String & source_dirs_str, const String & dest_dir);

    const ASTPtr & query_ptr;
    std::unordered_map<String, Field> user_settings;

private:
    StoragePtr storage = nullptr;

};

}
