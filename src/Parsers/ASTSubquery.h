#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{


/** SELECT subquery
  */
class ASTSubquery : public ASTWithAlias
{
public:
    // Stored the name when the subquery is defined in WITH clause. For example:
    // WITH (SELECT 1) AS a SELECT * FROM a AS b; cte_name will be `a`.
    std::string cte_name;

    // Stored the database name when the subquery is defined by a view. For example:
    // CREATE VIEW db1.v1 AS SELECT number FROM system.numbers LIMIT 10;
    // SELECT * FROM v1; database_of_view will be `db1`; cte_name will be `v1`.
    std::string database_of_view;
    /** Get the text that identifies this element. */
    String getID(char) const override { return "Subquery"; }

    ASTType getType() const override { return ASTType::ASTSubquery; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTSubquery>(*this);
        ASTPtr ptr{res};

        res->children.clear();

        for (const auto & child : children)
            res->children.emplace_back(child->clone());

        return ptr;
    }

    void updateTreeHashImpl(SipHash & hash_state) const override;

    bool isWithClause() const { return !cte_name.empty(); }

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
