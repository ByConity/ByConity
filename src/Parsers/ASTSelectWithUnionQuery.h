#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/** Single SELECT query or multiple SELECT queries with UNION
 * or UNION or UNION DISTINCT
  */
class ASTSelectWithUnionQuery : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "SelectWithUnionQuery"; }

    ASTType getType() const override { return ASTType::ASTSelectWithUnionQuery; }

    ASTPtr clone() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void collectAllTables(std::vector<ASTPtr> &, bool &) const;

    void resetTEALimit();

    enum class Mode
    {
        Unspecified,
        ALL,
        DISTINCT,
        EXCEPT_UNSPECIFIED,
        EXCEPT_ALL,
        EXCEPT_DISTINCT,
        INTERSECT_UNSPECIFIED,
        INTERSECT_ALL,
        INTERSECT_DISTINCT
    };

    using UnionModes = std::vector<Mode>;
    using UnionModesSet = std::unordered_set<Mode>;

    Mode union_mode;

    UnionModes list_of_modes;

    bool is_normalized = false;

    ASTPtr list_of_selects;

    // special info for TEA LIMIT post stage processing
    ASTPtr tealimit;

    UnionModesSet set_of_modes;

    /// Consider any mode other than ALL as non-default.
    bool hasNonDefaultUnionMode() const;

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);
};

}
