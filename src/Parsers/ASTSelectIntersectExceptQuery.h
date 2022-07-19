#pragma once

#include <Parsers/ASTSelectQuery.h>


namespace DB
{

class ASTSelectIntersectExceptQuery : public ASTSelectQuery
{
public:
    String getID(char) const override { return "SelectIntersectExceptQuery"; }

    ASTType getType() const override { return ASTType::ASTSelectIntersectExceptQuery; }

    ASTPtr clone() const override;

    enum class Operator
    {
        UNKNOWN,
        INTERSECT_ALL,
        INTERSECT_DISTINCT,
        EXCEPT_ALL,
        EXCEPT_DISTINCT
    };

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTs getListOfSelects() const;

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);
};

}
