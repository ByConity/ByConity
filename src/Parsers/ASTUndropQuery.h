#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/** UNDROP query
  */
class ASTUndropQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    /** Get the text that identifies this element. */
    String getID(char) const override;
    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTUndropQuery>(clone(), new_database);
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
