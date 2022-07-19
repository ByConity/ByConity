#include <QueryPlan/SymbolMapper.h>

#include <Optimizer/SimpleExpressionRewriter.h>

namespace DB
{
class SymbolMapper::IdentifierRewriter : public SimpleExpressionRewriter<Void>
{
public:
    explicit IdentifierRewriter(MappingFunction & mapping_function_) : mapping_function(mapping_function_) { }

    ASTPtr visitASTIdentifier(ASTPtr & expr, Void &) override
    {
        return std::make_shared<ASTIdentifier>(mapping_function(expr->as<ASTIdentifier &>().name()));
    }

private:
    MappingFunction & mapping_function;
};


SymbolMapper SymbolMapper::symbolMapper(const std::unordered_map<Symbol, Symbol> & mapping)
{
    return SymbolMapper([&mapping](Symbol symbol) {
        while (mapping.contains(symbol) && mapping.at(symbol) != symbol)
        {
            symbol = mapping.at(symbol);
        }
        return symbol;
    });
}

SymbolMapper SymbolMapper::symbolReallocator(std::unordered_map<Symbol, Symbol> & mapping, SymbolAllocator & symbolAllocator)
{
    return SymbolMapper([&](Symbol symbol) {
        if (mapping.contains(symbol))
        {
            while (mapping.contains(symbol) && mapping.at(symbol) != symbol)
            {
                symbol = mapping.at(symbol);
            }
            // do not remap the symbol further
            mapping.emplace(symbol, symbol);
            return symbol;
        }
        Symbol new_symbol = symbolAllocator.newSymbol(symbol);
        mapping.emplace(symbol, new_symbol);
        // do not remap the symbol further
        mapping.emplace(new_symbol, new_symbol);
        return new_symbol;
    });
}

Names SymbolMapper::map(const Names & symbols)
{
    Names ret;
    std::transform(symbols.begin(), symbols.end(), std::back_inserter(ret), mapping_function);
    return ret;
}

NameSet SymbolMapper::mapToDistinct(const Names & symbols)
{
    NameSet ret;
    std::transform(symbols.begin(), symbols.end(), std::inserter(ret, ret.end()), mapping_function);
    return ret;
}

NamesAndTypes SymbolMapper::map(const NamesAndTypes & name_and_types)
{
    NamesAndTypes ret;
    std::transform(name_and_types.begin(), name_and_types.end(), std::back_inserter(ret), [&](const auto & name_and_type) {
        return NameAndTypePair{mapping_function(name_and_type.name), name_and_type.type};
    });
    return ret;
}

Block SymbolMapper::map(const Block & name_and_types)
{
    Block ret;
    for (const auto & item : name_and_types)
    {
        ret.insert(ColumnWithTypeAndName{item.column, item.type, mapping_function(item.name)});
    }
    return ret;
}

DataStreams SymbolMapper::map(const DataStreams & data_streams)
{
    DataStreams ret;
    std::transform(
        data_streams.begin(), data_streams.end(), std::back_inserter(ret), [this](const auto & data_stream) { return map(data_stream); });
    return ret;
}

DataStream SymbolMapper::map(const DataStream & data_stream)
{
    return DataStream{map(data_stream.header)};
}

ASTPtr SymbolMapper::map(const ASTPtr & expr)
{
    IdentifierRewriter visitor(mapping_function);
    Void context{};
    return ASTVisitorUtil::accept(expr->clone(), visitor, context);
}

ASTPtr SymbolMapper::map(const ConstASTPtr & expr)
{
    IdentifierRewriter visitor(mapping_function);
    Void context{};
    return ASTVisitorUtil::accept(expr->clone(), visitor, context);
}

std::shared_ptr<JoinStep> SymbolMapper::map(const JoinStep & join)
{
    return std::make_shared<JoinStep>(
        map(join.getInputStreams()),
        map(join.getOutputStream()),
        join.getKind(),
        join.getStrictness(),
        map(join.getLeftKeys()),
        map(join.getRightKeys()),
        map(join.getFilter()),
        join.isHasUsing(),
        join.getRequireRightKeys(),
        join.getAsofInequality(),
        join.getDistributionType(),
        join.isMagic());
}

}
