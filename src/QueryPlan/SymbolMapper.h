#pragma once

#include <Core/Names.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/ProjectionStep.h>

namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;

// todo@kaixi: implement for all plan nodes
class SymbolMapper
{
public:
    using Symbol = std::string;
    using MappingFunction = std::function<std::string(const std::string &)>;

    explicit SymbolMapper(MappingFunction mapping_function_) : mapping_function(std::move(mapping_function_)) { }

    static SymbolMapper symbolMapper(const std::unordered_map<Symbol, Symbol> & mapping);
    static SymbolMapper symbolReallocator(std::unordered_map<Symbol, Symbol> & mapping, SymbolAllocator & symbolAllocator);

    std::string map(const std::string & symbol) { return mapping_function(symbol); }
    Names map(const Names & symbols);
    NameSet mapToDistinct(const Names & symbols);
    NamesAndTypes map(const NamesAndTypes & name_and_types);
    Block map(const Block & name_and_types);
    DataStreams map(const DataStreams & data_streams);
    DataStream map(const DataStream & data_stream);
    ASTPtr map(const ASTPtr & expr);
    ASTPtr map(const ConstASTPtr & expr);

    std::shared_ptr<JoinStep> map(const JoinStep & join);

private:
    MappingFunction mapping_function;

    class IdentifierRewriter;
};

}
