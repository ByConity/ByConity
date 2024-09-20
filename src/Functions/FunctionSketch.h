#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSketchBinary.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSketchBinary.h>
#include <Columns/ColumnVector.h>
#include <Common/SketchAllocator.h>
#include <Core/Field.h>

#include <DataSketches/quantiles_sketch.hpp>
#include <DataSketches/hll.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


class FunctionQuantilesSketch : public IFunction
{
public:
    static constexpr auto name = "doubleQuantilesSketchEstimate";

    FunctionQuantilesSketch(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionQuantilesSketch>(context);
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Illegal argument size of function " + getName(),
                            ErrorCodes::BAD_ARGUMENTS);

        const DataTypeArray * arg0 = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!arg0)
            throw Exception("Argument 0 for function " + getName() + " must be an array but it has type "
                                + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!checkAndGetDataType<DataTypeFloat64>(arg0->getNestedType().get()))
            throw Exception("Argument 1 for function " + getName() + " must be an array of float64 but it has type array of "
                                + arguments[0]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeNullable * arg1 = checkAndGetDataType<DataTypeNullable>(arguments[1].get());
        if (arg1)
        {
            if (!checkAndGetDataType<DataTypeSketchBinary>(arg1->getNestedType().get()))
                throw Exception(
                    "Argument 1 for function " + getName() + " must be a sketch but it has type " + arguments[1]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
        }
        else
        {
            if (!checkAndGetDataType<DataTypeSketchBinary>(arguments[1].get()))
                throw Exception(
                    "Argument 1 for function " + getName() + " must be a sketch but it has type " + arguments[1]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (isColumnConst(*arguments[0].column))
        {
            const ColumnConst & column_const = static_cast<const ColumnConst &>(*arguments[0].column);
            ColumnPtr column_nested = column_const.getDataColumnPtr()->convertToFullColumnIfLowCardinality();

            if (typeid_cast<const ColumnArray *>(column_nested.get()))
            {
                const ColumnArray & column_array = static_cast<const ColumnArray &>(*column_nested);
                const ColumnVector<Float64> * column_detail = typeid_cast<const ColumnVector<Float64> *>(&(column_array.getData()));



                if (arguments[1].column->isNullable())
                {
                    auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
                    auto result_column = result_type->createColumn();

                    const auto & nullable_sketch = static_cast<const ColumnNullable &>(*arguments[1].column);
                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        if (nullable_sketch.isNullAt(i))
                        {
                            Array array;
                            for (size_t j = 0; j < column_detail->size(); j++)
                            {
                                array.push_back(Field());
                            }
                            result_column->insert(array);
                        }
                        else
                        {
                            Array array;
                            datasketches::quantiles_sketch<Float64> quantiles_sketch_data = datasketches::quantiles_sketch<Float64>::deserialize(nullable_sketch.getDataAt(i).data, nullable_sketch.getDataAt(i).size, datasketches::serde<Float64>(), std::less<Float64>(), AggregateFunctionHllSketchAllocator());

                            for (size_t j = 0; j < column_detail->size(); j++)
                            {
                                Float64 quantile_res = quantiles_sketch_data.get_quantile(column_detail->getFloat64(j));
                                array.push_back(quantile_res);
                            }
                            result_column->insert(array);
                        }
                    }
                    return result_column;
                }
                else
                {
                    auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
                    auto result_column = result_type->createColumn();
                    const auto & value = static_cast<const ColumnSketchBinary &>(*arguments[1].column);
                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        Array array;
                        datasketches::quantiles_sketch<Float64> quantiles_sketch_data = datasketches::quantiles_sketch<Float64>::deserialize(value.getDataAt(i).data, value.getDataAt(i).size, datasketches::serde<Float64>(), std::less<Float64>(), AggregateFunctionHllSketchAllocator());
                        for (size_t j = 0; j < column_detail->size(); j++)
                        {
                            Float64 quantile_res = quantiles_sketch_data.get_quantile(column_detail->getFloat64(j));
                            array.push_back(quantile_res);
                        }
                        result_column->insert(array);
                    }

                    return result_column;
                }


            }
            else throw Exception("Argument 0 for function " + getName() + " must be const array of float64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else throw Exception("Argument 0 for function " + getName() + " must be const array of float64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    ContextPtr context;
};


class FunctionHLLSketch : public IFunction
{
public:
    static constexpr auto name = "doubleHllSketchEstimate";

    FunctionHLLSketch(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionHLLSketch>(context);
    }

    std::string getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Illegal argument size of function " + getName(),
                            ErrorCodes::BAD_ARGUMENTS);

        const DataTypeNullable * arg0 = checkAndGetDataType<DataTypeNullable>(arguments[0].get());
        if (arg0)
        {
            if (!checkAndGetDataType<DataTypeSketchBinary>(arg0->getNestedType().get()))
                throw Exception(
                    "Argument 1 for function " + getName() + " must be a sketch but it has type " + arguments[0]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
        }
        else
        {
            if (!checkAndGetDataType<DataTypeSketchBinary>(arguments[0].get()))
                throw Exception(
                    "Argument 1 for function " + getName() + " must be a sketch but it has type " + arguments[0]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeFloat64>();
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        bool use_composite_estimate = arguments.size() > 1 ? true : false;
        if (arguments[0].column->isNullable())
        {
            auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
            auto result_column = result_type->createColumn();

            const auto & nullable_sketch = static_cast<const ColumnNullable &>(*arguments[0].column);
            for (size_t i = 0; i < input_rows_count; i++)
            {
                if (nullable_sketch.isNullAt(i))
                {
                    result_column->insert(Field());
                }
                else
                {
                    datasketches::hll_sketch hll_sketch_data = datasketches::hll_sketch::deserialize(nullable_sketch.getDataAt(i).data, nullable_sketch.getDataAt(i).size, AggregateFunctionHllSketchAllocator());
                    if (use_composite_estimate)
                        result_column->insert(hll_sketch_data.get_composite_estimate());
                    else
                        result_column->insert(hll_sketch_data.get_estimate());
                }
            }
            return result_column;
        }
        else
        {
            auto result_column = ColumnVector<Float64>::create();
            auto & dst_data = result_column->getData();
            dst_data.resize(input_rows_count);

            const auto & value_column = static_cast<const ColumnSketchBinary &>(*arguments[0].column);
            for (size_t i = 0; i < input_rows_count; i++)
            {
                auto value = value_column.getDataAt(i);
                datasketches::hll_sketch hll_sketch_data = datasketches::hll_sketch::deserialize(value.data, value.size, AggregateFunctionHllSketchAllocator());
                if (use_composite_estimate)
                    dst_data[i] = hll_sketch_data.get_composite_estimate();
                else
                    dst_data[i] = hll_sketch_data.get_estimate();
            }

            return result_column;
        }

    }

private:
    ContextPtr context;
};

}
