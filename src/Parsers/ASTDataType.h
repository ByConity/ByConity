#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTDataType : public IAST
{
public:
    using IAST::IAST;

    explicit ASTDataType(const ASTPtr &, bool = false);

    /** Get text identifying the AST node. */
    String getID(char delim) const override
    {
        return std::string("DT") + delim + (nullable ? "N" : "NN") +
               delim + data_type->getID(delim);
    }

    ASTType getType() const override { return ASTType::ASTDataType; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    bool getNullable() const { return nullable; }

    ASTPtr getNestedType() const { return data_type; }

    void serialize(WriteBuffer &) const override;
    void deserializeImpl(ReadBuffer &) override;
    static ASTPtr deserialize(ReadBuffer &);

private:
    ASTPtr data_type;
    bool nullable;
};

}

