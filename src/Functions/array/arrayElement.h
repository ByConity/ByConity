#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ArrayImpl
{
    class NullMapBuilder;
}

/** arrayElement(arr, i) - get the array element by index. If index is not constant and out of range - return default value of data type.
  * The index begins with 1. Also, the index can be negative - then it is counted from the end of the array.
  *
  * under mysql dialect, if the first argument is array
  * * always returns nullable result
  * * throw exceptions if the index is 0
  * * returns NULL if the index is NULL or out of range
  */
class FunctionArrayElement : public IFunction
{
public:
    static constexpr auto name = "arrayElement";
    static FunctionPtr create(ContextPtr context);

    explicit FunctionArrayElement(ContextPtr context)
    {
        is_mysql_dialect = context && context->getSettingsRef().dialect_type == DialectType::MYSQL;
    }

    String getName() const override;

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return !is_mysql_dialect; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool is_mysql) const;

    friend class FunctionMapElement;

    bool is_mysql_dialect;

    ColumnPtr perform(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                      ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const;

    template <typename DataType>
    static ColumnPtr executeNumberConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType, typename DataType>
    static ColumnPtr executeNumber(const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder & builder);

    static ColumnPtr executeStringConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    static ColumnPtr executeString(const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder & builder);

    static ColumnPtr executeGenericConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    static ColumnPtr executeGeneric(const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder & builder);

    template <typename IndexType>
    static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                  const PaddedPODArray <IndexType> & indices, ArrayImpl::NullMapBuilder & builder,
                                  size_t input_rows_count);

    template <typename IndexType>
    ColumnPtr executeArgument(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                              ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const;

    /** For a tuple array, the function is evaluated component-wise for each element of the tuple.
      */
    ColumnPtr executeTuple(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    /** For a map the function finds the matched value for a key.
     *  Currently implemented just as linear search in array.
     *  However, optimizations are possible.
     */
    ColumnPtr executeMap(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, ColumnPtr index_null_map_col) const;

    using Offsets = ColumnArray::Offsets;

    static bool matchKeyToIndexNumber(
        const IColumn & data, const Offsets & offsets, bool is_key_const,
        const IColumn & index, PaddedPODArray<UInt64> & matched_idxs);

    static bool matchKeyToIndexNumberConst(
        const IColumn & data, const Offsets & offsets,
        const Field & index, PaddedPODArray<UInt64> & matched_idxs);

    static bool matchKeyToIndexString(
        const IColumn & data, const Offsets & offsets, bool is_key_const,
        const IColumn & index, PaddedPODArray<UInt64> & matched_idxs);

    static bool matchKeyToIndexStringConst(
        const IColumn & data, const Offsets & offsets,
         const Field & index, PaddedPODArray<UInt64> & matched_idxs);

     template <typename Matcher>
     static void executeMatchKeyToIndex(const Offsets & offsets,
         PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher);

    template <typename Matcher>
    static void executeMatchConstKeyToIndex(
        size_t num_rows, size_t num_values,
        PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher);
};

}
