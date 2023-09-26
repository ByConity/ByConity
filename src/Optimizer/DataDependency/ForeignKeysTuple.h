#pragma once
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Core/Types.h>
#include <Optimizer/Property/Equivalences.h>
#include <common/types.h>
#include <Core/Names.h>

namespace DB
{
using SymbolEquivalences = Equivalences<String>;

struct FkToPKPair
{
    String fk_column_name;
    String ref_table_name;
    String ref_column_name;

    FkToPKPair(const String & fk_column_name_, const String & ref_table_name_, const String & ref_column_name_)
        : fk_column_name(fk_column_name_), ref_table_name(ref_table_name_), ref_column_name(ref_column_name_)
    {
    }

    FkToPKPair(const FkToPKPair & other)
    {
        fk_column_name = other.fk_column_name;
        ref_table_name = other.ref_table_name;
        ref_column_name = other.ref_column_name;
    }

    bool operator==(const FkToPKPair & pair) const
    {
        return fk_column_name == pair.fk_column_name && ref_table_name == pair.ref_table_name && ref_column_name == pair.ref_column_name;
    }

    std::tuple<String, String, String> operator()() const
    {
        return std::make_tuple(fk_column_name, ref_table_name, ref_column_name);
    }
};


class OrdinaryKey
{
public:
    OrdinaryKey(String tbl_name_, String column_name_)
        : tbl_name(std::move(tbl_name_)), column_name(std::move(column_name_))
    {
        current_name = column_name;
    }

    OrdinaryKey copy(const String & new_current_name = "") const
    {
        auto result = OrdinaryKey(tbl_name, column_name);
        result.current_name = new_current_name.empty() ? current_name : new_current_name;
        return result;
    }

    String getTableName() const
    {
        return tbl_name;
    }

    String getColumnName() const
    {
        return column_name;
    }

    String getCurrentName() const
    {
        return current_name;
    }

    bool operator==(const OrdinaryKey & other) const
    {
        return tbl_name == other.tbl_name && column_name == other.column_name && current_name == other.current_name;
    }

    struct hash
    {
        std::size_t operator()(const OrdinaryKey & c) const
        {
            return std::hash<std::string>()(c.tbl_name) ^ std::hash<std::string>()(c.column_name)
                ^ std::hash<std::string>()(c.current_name);
        }
    };

private:
    String tbl_name;
    String column_name;
    String current_name;
};

class ForeignKeyOrPrimaryKey
{
public:
    enum class KeyType
    {
        PRIMARY_KEY,
        FOREIGN_KEY
    };

    ForeignKeyOrPrimaryKey(KeyType key_type_, String tbl_name_, String column_name_)
        : key_type(key_type_), tbl_name(std::move(tbl_name_)), column_name(std::move(column_name_))
    {
        current_name = column_name;
    }

    bool isPrimaryKey() const
    {
        return key_type == KeyType::PRIMARY_KEY;
    }

    bool isForeignKey() const
    {
        return key_type == KeyType::FOREIGN_KEY;
    }

    ForeignKeyOrPrimaryKey copy(const String & new_current_name = "") const
    {
        ForeignKeyOrPrimaryKey result(key_type, tbl_name, column_name);
        result.current_name = new_current_name.empty() ? current_name : new_current_name;
        return result;
    }

    String getTableName() const
    {
        return tbl_name;
    }

    String getColumnName() const
    {
        return column_name;
    }

    String getCurrentName() const
    {
        return current_name;
    }

    bool operator==(const ForeignKeyOrPrimaryKey & other) const
    {
        return key_type == other.key_type && tbl_name == other.tbl_name && column_name == other.column_name && current_name == other.current_name;
    }

    struct hash
    {
        std::size_t operator()(const ForeignKeyOrPrimaryKey & c) const
        {
            return std::hash<int>()(static_cast<int>(c.key_type)) ^ std::hash<std::string>()(c.tbl_name) ^ std::hash<std::string>()(c.column_name)
                ^ std::hash<std::string>()(c.current_name);
        }
    };

private:
    KeyType key_type;
    String tbl_name;
    String column_name;
    String current_name;
};


template <typename C>
concept HasContainsMethod = requires(C c) { c.contains(C::value_type); };

class OrdinaryKeys : public std::unordered_set<OrdinaryKey, OrdinaryKey::hash>
{
public:
    OrdinaryKeys getKeysInTable(const String & tbl_name) const
    {
        OrdinaryKeys result;
        for (const auto & key : *this)
        {
            if (tbl_name == key.getTableName())
                result.insert(key);
        }
        return result;
    }

    std::set<String> getCurrentNamesInTable(const String & tbl_name) const
    {
        NameOrderedSet result;
        for (const auto & key : *this)
        {
            if (tbl_name.empty() || tbl_name == key.getTableName())
                result.insert(key.getCurrentName());
        }
        return result;
    }

    template<typename C> 
    OrdinaryKeys getKeysInCurrentNames(const C & current_names) const
    {
        NameSet name_set;
        if constexpr (!HasContainsMethod<C>)
        {
            name_set.insert(current_names.begin(), current_names.end());
        }
        else
        {
            OrdinaryKeys result;
            for (const auto & key : *this)
            {
                if (current_names.contains(key.getCurrentName()))
                    result.insert(key);
            }
            return result;
        }

        OrdinaryKeys result;
        for (const auto & key : *this)
        {
            if (name_set.contains(key.getCurrentName()))
                result.insert(key);
        }
        return result;
    }

    void
    updateKey(const String & tbl_name, const String & column_name)
    {
        emplace(tbl_name, column_name);
    }

    OrdinaryKeys translate(const std::unordered_map<String, String> & identities) const
    {
        OrdinaryKeys result;
        for (const auto & fp : *this)
        {
            if (identities.contains(fp.getCurrentName()))
            {
                auto new_fp = fp.copy(identities.at(fp.getCurrentName()));
                result.emplace(new_fp);
            }
        }
        return result;
    }

    OrdinaryKeys normalize(const SymbolEquivalences & symbol_equivalences) const
    {
        auto mapping = symbol_equivalences.representMap();
        for (const auto & fp : *this)
        {
            if (!mapping.contains(fp.getCurrentName()))
            {
                mapping[fp.getCurrentName()] = fp.getCurrentName();
            }
        }
        return translate(mapping);
    }

};

class ForeignKeyOrPrimaryKeys : public std::unordered_set<ForeignKeyOrPrimaryKey, ForeignKeyOrPrimaryKey::hash>
{
public:
    void downgrade(const NameSet & invalid_tables)
    {
        std::erase_if(*this, [&](const ForeignKeyOrPrimaryKey & key){ return key.isPrimaryKey() && invalid_tables.contains(key.getTableName()); });
    }

    NameSet downgradeAll()
    {
        NameSet invalid_tables;
        std::erase_if(*this, [&](const ForeignKeyOrPrimaryKey & key){ 
            if (key.isPrimaryKey())
            {
                invalid_tables.insert(key.getTableName());
                return true;
            }
            return false;
        });
        return invalid_tables;
    }

    bool containsByTableColumn(const ForeignKeyOrPrimaryKey & target) const
    {
        for (const auto & key : *this)
            if (key.getTableName() == target.getTableName() && key.getColumnName() == target.getColumnName())
                return true;
        return false;
    }

    ForeignKeyOrPrimaryKeys getKeysInTable(const String & tbl_name) const
    {
        ForeignKeyOrPrimaryKeys result;
        for (const auto & key : *this)
        {
            if (tbl_name == key.getTableName())
                result.insert(key);
        }
        return result;
    }

    std::set<String> getCurrentNamesInTable(const String & tbl_name) const
    {
        NameOrderedSet result;
        for (const auto & key : *this)
        {
            if (tbl_name.empty() || tbl_name == key.getTableName())
                result.insert(key.getCurrentName());
        }
        return result;
    }

    template<typename C> 
    ForeignKeyOrPrimaryKeys getKeysInCurrentNames(const C & current_names) const
    {
        NameSet name_set;
        if constexpr (!HasContainsMethod<C>)
        {
            name_set.insert(current_names.begin(), current_names.end());
        }
        else
        {
            ForeignKeyOrPrimaryKeys result;
            for (const auto & key : *this)
            {
                if (current_names.contains(key.getCurrentName()))
                    result.insert(key);
            }
            return result;
        }

        ForeignKeyOrPrimaryKeys result;
        for (const auto & key : *this)
        {
            if (name_set.contains(key.getCurrentName()))
                result.insert(key);
        }
        return result;
    }

    ForeignKeyOrPrimaryKeys getForeignKeySet() const
    {
        ForeignKeyOrPrimaryKeys result;
        for (const auto & key : *this)
        {
            if (key.isForeignKey())
                result.insert(key);
        }
        return result;
    }

    ForeignKeyOrPrimaryKeys getPrimaryKeySet() const
    {
        ForeignKeyOrPrimaryKeys result;
        for (const auto & key : *this)
        {
            if (key.isPrimaryKey())
                result.insert(key);
        }
        return result;
    }

    void
    updateKey(ForeignKeyOrPrimaryKey::KeyType key_type, const String & tbl_name, const String & column_name)
    {
        emplace(key_type, tbl_name, column_name);
    }

    ForeignKeyOrPrimaryKeys translate(const std::unordered_map<String, String> & identities) const
    {
        ForeignKeyOrPrimaryKeys result;
        for (const auto & fp : *this)
        {
            if (identities.contains(fp.getCurrentName()))
            {
                auto new_fp = fp.copy(identities.at(fp.getCurrentName()));
                result.emplace(new_fp);
            }
            else
            {
                result.emplace(fp.copy());
            }
        }
        return result;
    }

    ForeignKeyOrPrimaryKeys normalize(const SymbolEquivalences & symbol_equivalences) const
    {
        auto mapping = symbol_equivalences.representMap();
        for (const auto & fp : *this)
        {
            if (!mapping.contains(fp.getCurrentName()))
            {
                mapping[fp.getCurrentName()] = fp.getCurrentName();
            }
        }
        return translate(mapping);
    }
};

struct TableColumn
{
    String tbl_name;
    String column_name;

    bool empty() const
    {
        return tbl_name.empty() && column_name.empty();
    }

    bool operator==(const TableColumn & other) const
    {
        return tbl_name == other.tbl_name && column_name == other.column_name;
    }

    struct hash
    {
        size_t operator()(const TableColumn & t) const
        {
            return std::hash<std::string>()(t.tbl_name) ^ std::hash<std::string>()(t.column_name);
        }
    };
};

struct TableColumnInfo
{
    explicit TableColumnInfo(bool is_valid_ = false) : is_valid(is_valid_)
    {
    }

    bool is_valid;
    std::unordered_map<String, ForeignKeyOrPrimaryKeys> table_columns;
    std::unordered_map<String, OrdinaryKeys> table_ordinary_columns;
    std::unordered_map<TableColumn, TableColumn, TableColumn::hash> fk_to_pk;
};

}
