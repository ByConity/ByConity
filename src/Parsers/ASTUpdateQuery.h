#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include "Parsers/IAST_fwd.h"

namespace DB
{

/*
* Unique table update statement.
UPDATE `tables` 
SET assignment_list 
WHERE where_condition 
[ORDER BY ...] 
[LIMIT ...] 

There are two kinds of statements depending on the type of `tables`:
1. when `tables` is a single table, we call it a UPDATE SINGLE TABLE.
2. when `tables` is multi tables based on JOIN expression, we call it a UPDATE JOIN. 
    And the first table (left table) is the target table of the update action.
*/

class ASTUpdateQuery : public ASTQueryWithTableAndOutput
{
public:

    String getID(char) const override;
    ASTPtr clone() const override;

    bool single_table = false;
    ASTPtr tables;
    ASTPtr assignment_list;
    ASTPtr where_condition;

    // optional expressions
    ASTPtr order_by_expr;
    ASTPtr limit_value;
    ASTPtr limit_offset;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
