#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** Element of expression: INTO <TOTAL_BUCKET_NUMBER> SPLIT_NUMBER <SPLIT_NUMBER> WITH_RANGE
  */
class ASTClusterByElement : public IAST
{
public:

    Int64 split_number;
    bool is_with_range;

    ASTClusterByElement() = default;

    ASTClusterByElement(ASTPtr columns_elem, ASTPtr total_bucket_number_elem, Int64 split_number_, bool is_with_range_)
        : split_number(split_number_), is_with_range(is_with_range_)
    {
        children.push_back(columns_elem);
        children.push_back(total_bucket_number_elem);
    }

    const ASTPtr & getColumns() const { return children.front(); }
    const ASTPtr & getTotalBucketNumber() const { return children.back(); }

    String getID(char) const override { return "ClusterByElement"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
