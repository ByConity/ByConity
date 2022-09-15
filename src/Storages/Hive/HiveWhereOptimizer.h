
#pragma once

#include <memory>
#include <unordered_map>
#include <set>
#include <boost/noncopyable.hpp>
#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

namespace DB
{

class ASTSelectQuery;
class StorageCnchHive;
using StoragePtr = std::shared_ptr<IStorage>;

class HiveWhereOptimizer : private boost::noncopyable
{
public:
    HiveWhereOptimizer(
        const SelectQueryInfo & query_info_,
        ContextPtr & /*context_*/,
        const StoragePtr & storage_);

    void implicitwhereOptimize() const;

    struct Condition
    {
        ASTPtr node;
        NameSet identifiers;
        bool viable = false;    //is partitionkey condition
        bool good = false;

        auto tuple() const
        {
            return std::make_tuple(!viable, !good);
        }

        bool operator< (const Condition & rhs) const
        {
            return tuple() < rhs.tuple();
        }

        Condition clone() const
        {
            Condition condition;
            condition.node = node->clone();
            condition.identifiers = identifiers;
            condition.viable = viable;
            condition.good = good;
            return condition;
        }
    };

    using Conditions = std::list<Condition>;

    ASTs getConditions(const ASTPtr & ast) const;

    ASTs getWhereOptimizerConditions(const ASTPtr & ast) const;

    void implicitAnalyzeImpl(Conditions & res, const ASTPtr & node) const;

    /// Transform conjunctions chain in WHERE expression to Conditions list.
    Conditions implicitAnalyze(const ASTPtr & expression) const;

    /// Transform Conditions list to WHERE or PREWHERE expression.
    ASTPtr reconstruct(const Conditions & conditions) const;

    bool isValidPartitionColumn(const IAST * condition) const;

    bool isSubsetOfTableColumns(const NameSet & identifiers) const;

    // bool hasColumnOfTableColumns(const HiveWhereOptimizer::Conditions & conditions) const;
    // bool hasColumnOfTableColumns() const;
    // bool isInTableColumns(const ASTPtr & node) const;

    bool getUsefullFilter(String & filter);
    bool ConvertImplicitWhereToUsefullFilter(String & filter);
    bool ConvertWhereToUsefullFilter(ASTs & conditions, String & filter);

private:

    using StringSet = std::unordered_set<String>;

    StringSet table_columns;
    ASTPtr partition_expr_ast;
    // const Context & context;
    ASTSelectQuery & select;
    const StoragePtr & storage;
};

}
