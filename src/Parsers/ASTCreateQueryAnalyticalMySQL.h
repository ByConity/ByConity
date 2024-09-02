#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTableOverrides.h>
#include <Interpreters/StorageID.h>
#include "Parsers/IAST_fwd.h"

namespace DB
{

class ASTFunction;
class ASTSetQuery;
class ASTExpressionList;
class ASTStorage;

class ASTStorageAnalyticalMySQL : public ASTStorage
{
public:
    bool broadcast = false;
    IAST * mysql_engine;
    IAST * mysql_primary_key;
    IAST * distributed_by;
    IAST * storage_policy;
    IAST * hot_partition_count;
    IAST * block_size;
    IAST * rt_engine;
    IAST * table_properties;
    IAST * mysql_partition_by;
    IAST * life_cycle;


    String getID(char) const override { return "Storage definition for Analytical MySQL"; }

    ASTType getType() const override { return ASTType::ASTStorageAnalyticalMySQL; }

    //ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

/// CREATE TABLE or ATTACH TABLE query
class ASTCreateQueryAnalyticalMySQL : public ASTCreateQuery
{
public:
    ASTStorageAnalyticalMySQL* mysql_storage = nullptr;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "CreateQueryAnalyticalMySQL" + (delim + database) + delim + table; }

    ASTType getType() const override { return ASTType::ASTCreateQueryAnalyticalMySQL; }

    ASTPtr transform() const;

    ASTPtr clone() const override;

    bool isView() const { return is_ordinary_view || is_materialized_view || is_live_view; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
