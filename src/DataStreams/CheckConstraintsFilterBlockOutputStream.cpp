#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/FilterDescription.h>
#include <DataStreams/CheckConstraintsFilterBlockOutputStream.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
    extern const int UNSUPPORTED_METHOD;
}


CheckConstraintsFilterBlockOutputStream::CheckConstraintsFilterBlockOutputStream(
    const StorageID & table_id_,
    const BlockOutputStreamPtr & output_,
    const Block & header_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : table_id(table_id_),
    output(output_),
    header(header_),
    constraints(constraints_),
    expressions(constraints_.getExpressions(context_, header.getNamesAndTypesList()))
{
}


void CheckConstraintsFilterBlockOutputStream::write(const Block & block)
{
    written_block = block;
    if (block.rows() > 0)
    {
        for (size_t expr_i = 0; expr_i < expressions.size(); ++expr_i)
        {
            Block block_to_calculate = written_block;
            auto constraint_expr = expressions[expr_i];
            constraint_expr->execute(block_to_calculate);

            auto * constraint_ptr = constraints.constraints[expr_i]->as<ASTConstraintDeclaration>();

            ColumnWithTypeAndName res_column = block_to_calculate.getByName(constraint_ptr->expr->getColumnName());

            auto result_type = removeNullable(removeLowCardinality(res_column.type));

            if (!isUInt8(result_type))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Constraint {} does not return a value of type UInt8",
                    backQuote(constraint_ptr->name));

            auto result_column = res_column.column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

            FilterDescription filter_and_holder(*result_column);

            size_t filtered_rows = countBytesInFilter(*filter_and_holder.data);
            /// If the current block is completely filtered out, let's move on to the next one.
            if (filtered_rows == 0)
            {
                return;
            }

            /// If all the rows pass through the filter.
            if (filtered_rows == filter_and_holder.data->size())
            {
                continue;
            }
            else
            {
                /// Filter columns.
                for (size_t i = 0; i < written_block.columns(); ++i)
                {
                    ColumnWithTypeAndName &current_column = written_block.safeGetByPosition(i);

                    if (isColumnConst(*current_column.column))
                        current_column.column = current_column.column->cut(0, filtered_rows);
                    else
                        current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
                }
            }
        }
    }

    output->write(written_block);
}

void CheckConstraintsFilterBlockOutputStream::flush()
{
    output->flush();
}

void CheckConstraintsFilterBlockOutputStream::writePrefix()
{
    output->writePrefix();
}

void CheckConstraintsFilterBlockOutputStream::writeSuffix()
{
    output->writeSuffix();
}

}
